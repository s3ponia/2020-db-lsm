package ru.mail.polis.s3ponia;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.stream.Collectors;

public class DiskManager {
    static final String META_EXTENSION = ".mdb";
    static final String TABLE_EXTENSION = ".db";
    private final Path metaFile;
    private int generation;

    private void saveTo(final Table dao, final Path file) throws IOException {
        if (!Files.exists(file)) {
            Files.createFile(file);
        }
        try (FileChannel writer = FileChannel.open(file, StandardOpenOption.WRITE)) {
            var shifts = new int[dao.size()];
            shifts[0] = 0;
            var index = 0;
            final var iterator = dao.iterator();
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

    private String getName() {
        ++generation;
        return Integer.toString(generation & ~(1 << (Integer.SIZE - 1)));
    }

    private void setLastGeneration() throws IOException {
        final var lines = Files.readAllLines(metaFile);
        if (lines.isEmpty()) {
            return;
        }
        final var lastFile = Paths.get(lines.get(lines.size() - 1)).getFileName().toString();
        generation = Integer.parseInt(lastFile.substring(0, lastFile.length() - 3)) + 1;
    }

    DiskManager(final Path file) throws IOException {
        metaFile = file;
        if (!Files.exists(metaFile)) {
            Files.createFile(metaFile);
        }
        setLastGeneration();
    }

    List<DiskTable> diskTables() throws IOException {
        return Files.readAllLines(metaFile).stream()
                .map(Paths::get)
                .map(DiskTable::of)
                .collect(Collectors.toList());
    }

    void save(final Table dao) throws IOException {
        try (var writer = Files.newBufferedWriter(this.metaFile, Charset.defaultCharset(), StandardOpenOption.APPEND)) {
            final var fileName = getName() + TABLE_EXTENSION;
            writer.write(Paths.get(metaFile.getParent().toString(), fileName) + "\n");
            saveTo(dao, Paths.get(metaFile.getParent().toString(), fileName));
        }
    }
}
