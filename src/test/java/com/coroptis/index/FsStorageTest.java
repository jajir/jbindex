package com.coroptis.index;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;

import org.junit.jupiter.api.Test;

import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.FileReader;
import com.coroptis.index.directory.FileWriter;
import com.coroptis.index.directory.FsDirectory;
import com.coroptis.index.directory.MemDirectory;

public class FsStorageTest {

    private final static String FILE_NAME = "pok.txt";

    private final static String TEXT = "Ahoj lidi!";

    private final static String TEXT_LONG = "Ahoj vsichni lidi!";

    @Test
    public void test_read_write_text_fs() throws Exception {
        Directory dir = new FsDirectory(new File("./target/pok"));
        test_read_write_text(dir);
    }

    @Test
    public void test_read_write_text_mem() throws Exception {
        Directory dir = new MemDirectory();
        test_read_write_text(dir);
    }

    private void test_read_write_text(final Directory dir) {
        try (final FileWriter fw = dir.getFileWriter(FILE_NAME)) {
            fw.write(TEXT.getBytes());
        }

        try (final FileReader fr = dir.getFileReader(FILE_NAME)) {
            byte[] bytes = new byte[TEXT.getBytes().length];
            final int loadedBytes = fr.read(bytes);

            String pok = new String(bytes);
            assertEquals(TEXT, pok);
            assertEquals(TEXT.getBytes().length, loadedBytes);
        }

    }

    @Test
    public void test_read_write_end_of_file_reached_mem() throws Exception {
        Directory dir = new MemDirectory();
        test_read_long_bytes(dir);
    }

    @Test
    public void test_read_write_end_of_file_reached_fs() throws Exception {
        Directory dir = new FsDirectory(new File("./target/pok"));
        test_read_long_bytes(dir);
    }

    private void test_read_long_bytes(final Directory dir) {
        try (final FileWriter fw = dir.getFileWriter(FILE_NAME)) {
            fw.write(TEXT.getBytes());
        }

        try (final FileReader fr = dir.getFileReader(FILE_NAME)) {
            byte[] bytes = new byte[TEXT_LONG.getBytes().length];

            final int loadedBytes = fr.read(bytes);
            assertEquals(-1, loadedBytes);
        }
    }

}
