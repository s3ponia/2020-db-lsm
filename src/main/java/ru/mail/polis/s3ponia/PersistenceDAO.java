package ru.mail.polis.s3ponia;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;
import ru.mail.polis.s3ponia.Table.Cell;
import ru.mail.polis.s3ponia.Table.Value;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.ArrayList;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class PersistenceDAO implements DAO {
    private final Table currTable = new Table();
    private final DiskManager manager;
    private ByteBuffer cacheLastKey = null;
    private ByteBuffer cacheLastValue = null;
    private static final long MIN_FREE_MEMORY = 128 * 1024 * 1024 / 16;
    private static final Logger logger = Logger.getLogger(DiskTable.class.getName());

    private static class DiskManager {
        static final String META_EXTENSION = ".mdb";
        static final String TABLE_EXTENSION = ".db";
        private final Path metaFile;
        private static int counter = 0;

        private void saveTo(final Table dao, final Path file) throws IOException {
            if (!Files.exists(file)) {
                Files.createFile(file);
            } else {
                throw new RuntimeException("Save to existing file");
            }
            try (FileChannel writer = FileChannel.open(file, StandardOpenOption.WRITE)) {
                var shifts = new int[dao.size()];
                shifts[0] = 0;
                var index = 0;
                final var iterator = dao.iterator();
                /*
                    Cell stored structure:
                        [DeadFlagTimeStamp][KeySize][Key][Value]

                    File structure:
                        [Cell][Cell][Cell]....[shifts][shitsSize]
                 */
                while (iterator.hasNext()) {
                    final var cell = iterator.next();
                    var nextShift = shifts[index];
                    final var key = cell.getKey();
                    final var value = cell.getValue();

                    nextShift += key.remaining() + value.getValue().remaining() + Long.BYTES /* Meta size */
                            + Integer.BYTES /* Shift size */;

                    writer.write(ByteBuffer.allocate(Long.BYTES).putLong(value.getDeadFlagTimeStamp()).flip());
                    writer.write(ByteBuffer.allocate(Integer.BYTES).putInt(key.remaining()).flip());
                    writer.write(key);
                    writer.write(value.getValue());

                    if (index < dao.size() - 1) {
                        shifts[++index] = nextShift;
                    }
                }

                final var buffer = ByteBuffer.allocate(shifts.length * Integer.BYTES);
                buffer.asIntBuffer().put(shifts).flip();
                writer.write(buffer);
                writer.write(ByteBuffer.allocate(Integer.BYTES).putInt(dao.size()).flip());
            }
        }

        private String getName(final Table dao) {
            return Integer.toString((++counter & ~(1 << (Integer.SIZE - 1))));
        }

        DiskManager(final Path file) throws IOException {
            metaFile = file;
            if (!Files.exists(metaFile)) {
                Files.createFile(metaFile);
            }
        }

        List<DiskTable> diskTables() throws IOException {
            return Files.readAllLines(this.metaFile).stream()
                    .map(Paths::get)
                    .map(DiskTable::of)
                    .collect(Collectors.toList());
        }

        void save(final Table dao) throws IOException {
            try (var writer = Files.newBufferedWriter(this.metaFile, Charset.defaultCharset(), StandardOpenOption.APPEND)) {
                final var fileName = getName(dao) + TABLE_EXTENSION;
                writer.write(Paths.get(metaFile.getParent().toString(), fileName) + "\n");
                saveTo(dao, Paths.get(metaFile.getParent().toString(), fileName));
            }
        }
    }

    /*
    Cell stored structure:
        [DeadFlagTimeStamp : long][KeySize : int][Key : ByteBuffer][Value : ByteBuffer]

    File structure:
        [Cell][Cell][Cell]....[shifts : int[]][shitsSize : int]
    */
    private static class DiskTable {
        private final int[] shifts;
        private final FileChannel fileChannel;

        private class DiskTableIterator implements Iterator<Cell> {
            private int elementIndex;

            private Cell getCell(final int index) throws IOException {
                if (index >= shifts.length - 1) {
                    throw new ArrayIndexOutOfBoundsException("Out of bound");
                }
                return readCell(getElementShift(index), getElementSize(index));
            }

            private int getElementIndex(@NotNull final ByteBuffer key) throws IOException {
                int left = 0;
                int right = shifts.length - 2;
                while (left <= right) {
                    final int mid = (left + right) / 2;
                    final ByteBuffer midKey = getCell(mid).getKey();
                    final int compareResult = midKey.compareTo(key);

                    if (compareResult < 0) {
                        left = mid + 1;
                    } else if (compareResult > 0) {
                        right = mid - 1;
                    } else {
                        return mid;
                    }
                }

                return left;
            }

            DiskTableIterator(@NotNull final ByteBuffer key) throws IOException {
                elementIndex = getElementIndex(key);
            }

            @Override
            public boolean hasNext() {
                return elementIndex < shifts.length - 1;
            }

            @Override
            public Cell next() {
                Cell result = null;
                try {
                    result = getCell(elementIndex);
                } catch (IOException e) {
                    logger.warning(e.toString());
                }
                ++elementIndex;
                return result;
            }
        }

        private int getElementSize(final int index) {
            if (index == shifts.length - 1) {
                return getShiftsArrayShift() - getElementShift(index);
            } else {
                return getElementShift(index + 1) - getElementShift(index);
            }
        }

        private int getShiftsArrayShift() {
            return shifts[shifts.length - 1];
        }

        private int getElementShift(final int index) {
            return shifts[index];
        }

        private Cell readCell(final ByteBuffer buff, final int size) {
            final var deadFlagTimeStamp = buff.getLong();
            final var keySize = buff.getInt();
            final var key = ByteBuffer.allocate(keySize);
            buff.limit(buff.position() + key.remaining());
            key.put(buff);
            buff.limit(buff.capacity());

            final var value = ByteBuffer.allocate(size - keySize - Integer.BYTES - Long.BYTES);
            value.put(buff);

            return Cell.of(key.flip(), Value.of(value.flip(), deadFlagTimeStamp));
        }

        private Cell readCell(final long position, final int size) throws IOException {
            final var buff = ByteBuffer.allocate(size);
            fileChannel.read(buff, position);

            return readCell(buff.flip(), size);
        }

        public DiskTable() {
            shifts = null;
            fileChannel = null;
        }

        DiskTable(final Path path) throws IOException {
            fileChannel = FileChannel.open(path, StandardOpenOption.READ);
            final long size = fileChannel.size();
            final var buffSize = ByteBuffer.allocate(Integer.BYTES);
            fileChannel.read(buffSize, size - Integer.BYTES);
            final var elementsQuantity = buffSize.flip().getInt();
            final var arrayShift = (int) size - Integer.BYTES * (elementsQuantity + 1);
            shifts = new int[elementsQuantity + 1];
            final var buff = ByteBuffer.allocate(Integer.BYTES * shifts.length);
            fileChannel.read(buff, arrayShift);
            buff.flip().asIntBuffer().get(shifts);
            shifts[elementsQuantity] = arrayShift;
        }

        public Iterator<Cell> iterator(@NotNull final ByteBuffer from) throws IOException {
            return new DiskTableIterator(from);
        }

        static DiskTable of(final Path path) {
            try {
                return new DiskTable(path);
            } catch (IOException e) {
                logger.warning(e.toString());
                return new DiskTable();
            }
        }
    }

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
