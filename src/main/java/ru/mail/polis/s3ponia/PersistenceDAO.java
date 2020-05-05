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
import java.util.NoSuchElementException;

public final class PersistenceDAO implements DAO {
    private final Table currTable = new Table();
    private final DiskManager manager;
    private ByteBuffer cacheLastKey;
    private ByteBuffer cacheLastValue;
    private static final long MIN_FREE_MEMORY = 128 * 1024 * 1024 / 16;
    private static final Logger logger = Logger.getLogger(PersistenceDAO.class.getName());

    private PersistenceDAO(final File data) throws IOException {
        this.manager = new DiskManager(Paths.get(data.getAbsolutePath(), data.getName() + DiskManager.META_EXTENSION));
    }

    private void flush() throws IOException {
        manager.save(currTable);
        currTable.close();
    }

    private void checkToFlush() throws IOException {
        if (Runtime.getRuntime().freeMemory() < MIN_FREE_MEMORY && currTable.size() > 0) {
            flush();
        }
    }

    public static PersistenceDAO of(final File data) throws IOException {
        return new PersistenceDAO(data);
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final var diskTables = manager.diskTables();
        final var diskIterators = new ArrayList<Iterator<Cell>>();
        diskIterators.add(currTable.iterator(from));
        diskTables.stream().forEach(diskTable -> {
            try {
                diskIterators.add(diskTable.iterator(from));
            } catch (IOException e) {
                logger.warning(e.toString());
            }
        });
        final var merge = Iterators.mergeSorted(diskIterators, Cell::compareTo);
        final var newest = Iters.collapseEquals(merge, Cell::getKey);
        final var removeDead = Iterators.filter(newest, el -> !el.getValue().isDead());

        return Iterators.transform(removeDead, c -> Record.of(c.getKey(), c.getValue().getValue()));
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull final ByteBuffer key) throws IOException {
        final var key_ = key.rewind().asReadOnlyBuffer();
        if (cacheLastKey != null && cacheLastKey.equals(key_)) {
            return cacheLastValue;
        }
        final Iterator<Record> iter;
        iter = iterator(key_);
        if (!iter.hasNext()) {
            throw new NoSuchElementException("Not found");
        }

        final Record next = iter.next();
        if (next.getKey().equals(key_)) {
            final var value = next.getValue();
            cacheLastKey = key_;
            cacheLastValue = value;
            return value;
        } else {
            throw new NoSuchElementException("Not found");
        }
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        currTable.upsert(key, value);
        cacheLastKey = key;
        cacheLastValue = value;
        checkToFlush();
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        currTable.remove(key);
        checkToFlush();
    }

    @Override
    public void close() throws IOException {
        if (currTable.size() > 0 && cacheLastValue != null && cacheLastKey != null) {
            flush();
        }
    }
}
