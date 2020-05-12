package ru.mail.polis.s3ponia;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.logging.Logger;

public class DiskTable {
    private static final Logger logger = Logger.getLogger(DiskTable.class.getName());
    private final int[] shifts;
    private final Path fileChannel;

    private class DiskTableIterator implements Iterator<Table.Cell> {
        private int elementIndex;

        private LazyCell getLazyCell(final int index) {
            if (index >= shifts.length - 1) {
                throw new ArrayIndexOutOfBoundsException("Out of bound");
            }
            return readLazyCell(getElementShift(index), getElementSize(index));
        }

        private int getElementIndex(@NotNull final ByteBuffer key) {
            int left = 0;
            int right = shifts.length - 2;
            while (left <= right) {
                final int mid = (left + right) / 2;
                final ByteBuffer midKey = getLazyCell(mid).getKey();
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

        DiskTableIterator(@NotNull final ByteBuffer key) {
            elementIndex = getElementIndex(key);
        }

        @Override
        public boolean hasNext() {
            return elementIndex < shifts.length - 1;
        }

        @Override
        public LazyCell next() {
            LazyCell result = null;
            result = getLazyCell(elementIndex);
            ++elementIndex;
            return result;
        }
    }

    private class LazyCell extends Table.Cell {
        final long position;
        final int size;

        public LazyCell(final long position, final int size) {
            super(null, null);
            this.position = position;
            this.size = size;
        }

        @NotNull
        public ByteBuffer getKey() {
            try (var channel = FileChannel.open(fileChannel, StandardOpenOption.READ)) {
                final var buffer = ByteBuffer.allocate(Integer.BYTES);
                channel.read(buffer, position + Long.BYTES);
                final var keySize = buffer.flip().getInt();
                final var key = ByteBuffer.allocate(keySize);
                channel.read(key, position + Long.BYTES + Integer.BYTES);
                return key.flip();
            } catch (IOException e) {
                logger.warning(e.toString());
                return ByteBuffer.allocate(0);
            }
        }

        @NotNull
        Table.Value getValue() {
            try (var channel = FileChannel.open(fileChannel, StandardOpenOption.READ)) {
                final var valueSizeBuf = ByteBuffer.allocate(Long.BYTES);
                channel.read(valueSizeBuf, position);
                final var deadFlagTimeStamp = valueSizeBuf.flip().getLong();
                final var buffer = ByteBuffer.allocate(Integer.BYTES);
                channel.read(buffer, position + Long.BYTES);
                final var keySize = buffer.flip().getInt();
                final var value = ByteBuffer.allocate(size - Integer.BYTES - Long.BYTES - keySize);
                channel.read(value, position + Long.BYTES + Integer.BYTES + keySize);

                return Table.Value.of(value.flip(), deadFlagTimeStamp);
            } catch (IOException e) {
                logger.warning(e.toString());
                return Table.Value.of(ByteBuffer.allocate(0));
            }
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

    private Table.Cell readCell(final ByteBuffer buff) {
        final var deadFlagTimeStamp = buff.getLong();
        final var keySize = buff.getInt();
        final var key = ByteBuffer.allocate(keySize);
        buff.limit(buff.position() + keySize);
        key.put(buff);
        buff.limit(buff.capacity());

        final var value = ByteBuffer.allocate(buff.remaining());
        value.put(buff);

        return Table.Cell.of(key.flip(), Table.Value.of(value.flip(), deadFlagTimeStamp));
    }

    private LazyCell readLazyCell(final long position, final int size) {
        return new LazyCell(position, size);
    }

    private Table.Cell readCell(final FileChannel channel,
                                final long position, final int size) throws IOException {
        final var buff = ByteBuffer.allocate(size);
        channel.read(buff, position);

        return readCell(buff.flip());
    }

    public DiskTable() {
        shifts = null;
        fileChannel = null;
    }

    DiskTable(final Path path) throws IOException {
        fileChannel = path;
        try (var channel = FileChannel.open(fileChannel, StandardOpenOption.READ)) {
            final long size = channel.size();
            final var buffSize = ByteBuffer.allocate(Integer.BYTES);
            channel.read(buffSize, size - Integer.BYTES);
            final var elementsQuantity = buffSize.flip().getInt();
            final var arrayShift = (int) size - Integer.BYTES * (elementsQuantity + 1);
            shifts = new int[elementsQuantity + 1];
            final var buff = ByteBuffer.allocate(Integer.BYTES * shifts.length);
            channel.read(buff, arrayShift);
            buff.flip().asIntBuffer().get(shifts);
            shifts[elementsQuantity] = arrayShift;
        }
    }

    public Iterator<Table.Cell> iterator(@NotNull final ByteBuffer from) {
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
