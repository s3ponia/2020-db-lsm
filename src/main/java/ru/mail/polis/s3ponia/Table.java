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
        private final ByteBuffer key;
        private final Value value;

        private Cell(@NotNull ByteBuffer key, @NotNull Value value) {
            this.key = key;
            this.value = value;
        }

        static Cell of(@NotNull ByteBuffer key, @NotNull Value value) {
            return new Cell(key, value);
        }

        @NotNull
        public ByteBuffer getKey() {
            return key;
        }

        @NotNull
        Value getValue() {
            return value;
        }

        @Override
        public int compareTo(@NotNull Cell o) {
            return Comparator.comparing(Cell::getKey).thenComparing(Cell::getValue).compare(this, o);
        }
    }

    public static class Value implements Comparable<Value> {
        private final ByteBuffer value;
        private final long DEAD_FLAG = 0x4000000000000000L;
        private final long deadFlagTimeStamp;

        private Value() {
            this.deadFlagTimeStamp = System.currentTimeMillis();
            this.value = ByteBuffer.allocate(0);
        }

        public Value(ByteBuffer value, long deadFlagTimeStamp) {
            this.value = value;
            this.deadFlagTimeStamp = deadFlagTimeStamp;
        }

        private Value(ByteBuffer value) {
            this.deadFlagTimeStamp = System.currentTimeMillis();
            this.value = value;
        }

        static Value of() {
            return new Value();
        }

        static Value of(ByteBuffer value) {
            return new Value(value.rewind());
        }

        static Value of(ByteBuffer value, long deadFlagTimeStamp) {
            return new Value(value, deadFlagTimeStamp);
        }

        ByteBuffer getValue() {
            return (value);
        }

        Value setDeadFlag() {
            return new Value(value, deadFlagTimeStamp | DEAD_FLAG);
        }

        Value unsetDeadFlag() {
            return new Value(value, deadFlagTimeStamp & ~DEAD_FLAG);
        }

        boolean isDead() {
            return (this.deadFlagTimeStamp & DEAD_FLAG) != 0;
        }

        public long getDeadFlagTimeStamp() {
            return deadFlagTimeStamp;
        }

        @Override
        public int compareTo(@NotNull Value o) {
            return Long.compare(o.getDeadFlagTimeStamp() & ~DEAD_FLAG, this.getDeadFlagTimeStamp() & ~DEAD_FLAG);
        }
    }

    public Table() {
        this.keyToRecord = new TreeMap<>();
    }

    public Table(TreeMap<ByteBuffer, Value> map) {
        keyToRecord = map;
    }

    public SortedMap<ByteBuffer, Value> getKeyToRecord() {
        return keyToRecord;
    }

    public int size() {
        return keyToRecord.size();
    }

    public Iterator<Cell> iterator() {
        return keyToRecord.entrySet().stream().map(e -> Cell.of(e.getKey(), e.getValue())).iterator();
    }

    @NotNull
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) {
        return keyToRecord.tailMap(from).entrySet().stream().map(
                e -> Cell.of(e.getKey(), e.getValue())
        ).iterator();
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
