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
    private final FileChannel fileChannel;

    private class DiskTableIterator implements Iterator<Table.Cell> {
        private int elementIndex;

        private Table.Cell getCell(final int index) throws IOException {
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
        public Table.Cell next() {
            Table.Cell result = null;
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

    private Table.Cell readCell(final ByteBuffer buff, final int size) {
        final var deadFlagTimeStamp = buff.getLong();
        final var keySize = buff.getInt();
        final var key = ByteBuffer.allocate(keySize);
        buff.limit(buff.position() + key.remaining());
        key.put(buff);
        buff.limit(buff.capacity());

        final var value = ByteBuffer.allocate(size - keySize - Integer.BYTES - Long.BYTES);
        value.put(buff);

        return Table.Cell.of(key.flip(), Table.Value.of(value.flip(), deadFlagTimeStamp));
    }

    private Table.Cell readCell(final long position, final int size) throws IOException {
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

    public Iterator<Table.Cell> iterator(@NotNull final ByteBuffer from) throws IOException {
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