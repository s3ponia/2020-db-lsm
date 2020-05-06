package ru.mail.polis.s3ponia;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

public class Table {
    private final SortedMap<ByteBuffer, Value> keyToRecord;

    public static class Cell implements Comparable<Cell> {
        @NotNull
        private final ByteBuffer key;
        @NotNull
        private final Value value;

        private Cell(@NotNull final ByteBuffer key, @NotNull final Value value) {
            this.key = key;
            this.value = value;
        }

        static Cell of(@NotNull final ByteBuffer key, @NotNull final Value value) {
            return new Cell(key, value);
        }

        @NotNull
        public ByteBuffer getKey() {
            return key.asReadOnlyBuffer();
        }

        @NotNull
        Value getValue() {
            return value;
        }

        @Override
        public int compareTo(@NotNull final Cell o) {
            return Comparator.comparing(Cell::getKey).thenComparing(Cell::getValue).compare(this, o);
        }
    }

    public static class Value implements Comparable<Value> {
        private final ByteBuffer byteBuffer;
        private static final long DEAD_FLAG = 0x4000000000000000L;
        private final long deadFlagTimeStamp;

        private Value() {
            this.deadFlagTimeStamp = System.currentTimeMillis();
            this.byteBuffer = ByteBuffer.allocate(0);
        }

        public Value(final ByteBuffer value, final long deadFlagTimeStamp) {
            this.byteBuffer = value;
            this.deadFlagTimeStamp = deadFlagTimeStamp;
        }

        private Value(final ByteBuffer value) {
            this.deadFlagTimeStamp = System.currentTimeMillis();
            this.byteBuffer = value;
        }

        static Value of() {
            return new Value();
        }

        static Value of(final ByteBuffer value) {
            return new Value(value);
        }

        static Value of(final ByteBuffer value, final long deadFlagTimeStamp) {
            return new Value(value, deadFlagTimeStamp);
        }

        ByteBuffer getValue() {
            return byteBuffer.asReadOnlyBuffer();
        }

        Value setDeadFlag() {
            return new Value(byteBuffer, deadFlagTimeStamp | DEAD_FLAG);
        }

        Value unsetDeadFlag() {
            return new Value(byteBuffer, deadFlagTimeStamp & ~DEAD_FLAG);
        }

        boolean isDead() {
            return (this.deadFlagTimeStamp & DEAD_FLAG) != 0;
        }

        public long getDeadFlagTimeStamp() {
            return deadFlagTimeStamp;
        }

        public long getTimeStamp() {
            return deadFlagTimeStamp & ~DEAD_FLAG;
        }

        @Override
        public int compareTo(@NotNull final Value o) {
            return Comparator.comparing(Value::getTimeStamp).reversed().compare(this, o);
        }
    }

    public Table() {
        this.keyToRecord = new TreeMap<>();
    }

    public int size() {
        return keyToRecord.size();
    }

    public Iterator<Cell> iterator() {
        return keyToRecord.entrySet().stream().map(e -> Cell.of(e.getKey(), e.getValue())).iterator();
    }

    /**
     * Provides iterator (possibly empty) over {@link Cell}s starting at "from" key (inclusive)
     * in <b>ascending</b> order according to {@link Cell#compareTo(Cell)}.
     * N.B. The iterator should be obtained as fast as possible, e.g.
     * one should not "seek" to start point ("from" element) in linear time ;)
     */
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) {
        return keyToRecord.tailMap(from).entrySet().stream().map(
                e -> Cell.of(e.getKey(), e.getValue())
        ).iterator();
    }

    public ByteBuffer get(@NotNull final ByteBuffer key) {
        final var val = keyToRecord.get(key);
        return val == null ? null : val.getValue();
    }

    public void upsert(@NotNull final ByteBuffer key, @NotNull final Value value) {
        keyToRecord.put(key, value);
    }

    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        keyToRecord.put(key, Value.of(value));
    }

    public void remove(@NotNull final ByteBuffer key) {
        keyToRecord.put(key, Value.of().setDeadFlag());
    }

    public void close() {
        keyToRecord.clear();
    }
}
