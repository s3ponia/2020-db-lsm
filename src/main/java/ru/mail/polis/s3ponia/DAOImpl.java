package ru.mail.polis.s3ponia;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class DAOImpl implements DAO {

    private final SortedMap<ByteBuffer, Record> keyToRecord = new TreeMap<>();

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        return keyToRecord.tailMap(from).values().stream().iterator();
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        keyToRecord.put(key, Record.of(key, value));
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        keyToRecord.remove(key);
    }

    @Override
    public void close() throws IOException {
        keyToRecord.clear();
    }
}
