package ru.mail.polis.s3ponia;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;
import ru.mail.polis.s3ponia.Table.Cell;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Logger;

public final class PersistenceDAO implements DAO {
    private final DiskManager manager;
    private final Table currTable;
    private final long maxMemory;
    private long currMemory;
    private static final long MIN_FREE_MEMORY = 128 * 1024 * 1024 / 32;
    private static final Logger logger = Logger.getLogger(PersistenceDAO.class.getName());

    private PersistenceDAO(final File data, final long maxMemory) throws IOException {
        this.manager = new DiskManager(Paths.get(data.getAbsolutePath(),
                DiskManager.META_PREFIX + data.getName() + DiskManager.META_EXTENSION));
        this.currTable = new Table(manager.getGeneration());
        this.maxMemory = maxMemory;
    }

    private void flush() throws IOException {
        manager.save(currTable);
        currMemory = 0;
        currTable.close();
    }

    private void checkToFlush() throws IOException {
        if (maxMemory - currMemory < MIN_FREE_MEMORY && currTable.size() > 0) {
            flush();
        }
    }

    public static PersistenceDAO of(final File data, final long memorySize) throws IOException {
        return new PersistenceDAO(data, memorySize);
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) {
        final var diskTables = manager.diskTables();
        final var diskIterators = new ArrayList<Iterator<Cell>>();
        diskIterators.add(currTable.iterator(from));
        diskTables.forEach(diskTable -> diskIterators.add(diskTable.iterator(from)));
        final var merge = Iterators.mergeSorted(diskIterators, Cell::compareTo);
        final var newest = Iters.collapseEquals(merge, Cell::getKey);
        final var removeDead = Iterators.filter(newest, el -> !el.getValue().isDead());

        return Iterators.transform(removeDead, c -> Record.of(c.getKey(), c.getValue().getValue()));
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        currMemory += key.limit() + value.limit() + Long.BYTES;
        checkToFlush();
        if (currMemory != 0) {
            currMemory -= key.limit() + value.limit() + Long.BYTES;
        }
        if (currTable.upsert(key, value)) {
            currMemory += key.limit() + value.limit() + Long.BYTES;
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        currMemory += key.limit();
        checkToFlush();
        if (currMemory != 0) {
            currMemory -= key.limit();
        }
        if (currTable.remove(key)) {
            currMemory += key.limit();
        }
    }

    @Override
    public void close() throws IOException {
        if (currTable.size() > 0) {
            flush();
        }
    }
}
