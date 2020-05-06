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
import java.util.Random;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class DiskManager {
    static final String META_EXTENSION = ".mdb";
    static final String META_PREFIX = "fzxyGZ9LDM";
    private final Path metaFile;
    private static final String TABLE_EXTENSION = ".db";
    private static final char MAGICK_NUMBER = 0xabc3;
    private final List<String> fileNames;
    private static final Logger logger = Logger.getLogger(DiskTable.class.getName());
    private final Random random = new Random(System.currentTimeMillis());

    private void saveTo(final Table dao, final Path file) throws IOException {
        Files.createFile(file);
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            logger.warning("Thread sleep error");
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

    private void setSeed() {
        if (fileNames.isEmpty()) {
            return;
        }
        random.setSeed(fileNames.hashCode());
    }

    private char getMagickNumber() {
        return MAGICK_NUMBER;
    }

    private String getName() {
        final byte[] randomBytes = new byte[200];
        random.nextBytes(randomBytes);
        final var name = UUID.nameUUIDFromBytes(randomBytes).toString();
        return name;
    }

    DiskManager(final Path file) throws IOException {
        if (Files.exists(file)) {
            boolean isMetaFile = true;
            try (var reader = Files.newBufferedReader(file)) {
                if (reader.read() != getMagickNumber()) {
                    isMetaFile = false;
                }
            } catch (IOException ex) {
                isMetaFile = false;
            }
            if (!isMetaFile) {
                Files.delete(file);
            }
        }
        metaFile = file;
        if (!Files.exists(metaFile)) {
            Files.createFile(metaFile);
            try (var writer = Files.newBufferedWriter(metaFile)) {
                writer.write(getMagickNumber());
                writer.write('\n');
            }
        }

        fileNames = Files.readAllLines(metaFile);

        setSeed();

    }

    List<DiskTable> diskTables() throws IOException {
        return fileNames.stream()
                .skip(1)
                .map(Paths::get)
                .map(DiskTable::of)
                .collect(Collectors.toList());
    }

    void save(final Table dao) throws IOException {
        try (var writer = Files.newBufferedWriter(this.metaFile, Charset.defaultCharset(), StandardOpenOption.APPEND)) {
            var filePath = Paths.get(metaFile.getParent().toString(), getName() + TABLE_EXTENSION);
            while (Files.exists(filePath)) {
                filePath = Paths.get(metaFile.getParent().toString(), getName() + TABLE_EXTENSION);
            }
            final var fileName = filePath.toString();
            fileNames.add(fileName);
            writer.write(fileName + "\n");
            saveTo(dao, filePath);
        }
    }
}
