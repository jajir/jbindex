package com.coroptis.index.directory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import com.coroptis.index.IndexException;

public class FsFileWriterStream implements FileWriter {

    private final OutputStream fio;

    private static final int BUFFER_SIZE = 1024 * 1024 * 5;

    FsFileWriterStream(final File file) {
        try {
            final Path path = file.toPath();
            final OutputStream os = Files.newOutputStream(path);
            this.fio = new BufferedOutputStream(os, BUFFER_SIZE);
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        try {
            fio.close();
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    @Override
    public void write(byte b) {
        try {
            fio.write(b);
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    @Override
    public void write(byte[] bytes) {
        try {
            fio.write(bytes);
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

}
