package com.coroptis.index;

import static org.junit.jupiter.api.Assertions.*;

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
	FileWriter fw = dir.getFileWriter(FILE_NAME);
	fw.write(TEXT.getBytes());
	fw.close();

	FileReader fr = dir.getFileReader(FILE_NAME);
	byte[] bytes = new byte[TEXT.getBytes().length];
	final int loadedBytes = fr.read(bytes);
	String pok = new String(bytes);
	fr.close();

	assertEquals(TEXT.getBytes().length, loadedBytes);
	assertEquals(TEXT, pok);
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
	FileWriter fw = dir.getFileWriter(FILE_NAME);
	fw.write(TEXT.getBytes());
	fw.close();

	FileReader fr = dir.getFileReader(FILE_NAME);
	byte[] bytes = new byte[TEXT_LONG.getBytes().length];

	final int loadedBytes = fr.read(bytes);
	fr.close();

	assertEquals(-1, loadedBytes);
    }

}
