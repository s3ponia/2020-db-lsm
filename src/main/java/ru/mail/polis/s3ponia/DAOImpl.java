package ru.mail.polis.s3ponia;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class DAOImpl implements DAO {

    private final TreeSet<Record> orderedRecords = new TreeSet<>();
    private final HashMap<ByteBuffer, Record> keyToRecord = new HashMap<>();

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final var record = keyToRecord.get(from);
        if(record == null) {
            return Collections.emptyIterator();
        } else {
            return orderedRecords.tailSet(keyToRecord.get(from)).iterator();
        }
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        final var record = Record.of(key, value);
        orderedRecords.remove(record);
        keyToRecord.put(key, record);
        orderedRecords.add(record);
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        final var record = keyToRecord.remove(key);
        orderedRecords.remove(record);
    }

    @Override
    public void close() throws IOException {
        orderedRecords.clear();
        keyToRecord.clear();
    }
}
