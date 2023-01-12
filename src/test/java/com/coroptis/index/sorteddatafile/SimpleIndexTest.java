package com.coroptis.index.sorteddatafile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.util.Comparator;

import org.junit.jupiter.api.Test;

import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.FsDirectory;
import com.coroptis.index.directory.MemDirectory;
import com.coroptis.index.type.TypeDescriptorByte;
import com.coroptis.index.type.TypeDescriptorString;

public class SimpleIndexTest {

    private final static String FILE_NAME = "pok.dat";
    private final TypeDescriptorByte byteTd = new TypeDescriptorByte();
    private final TypeDescriptorString stringTd = new TypeDescriptorString();

    @Test
    public void read_and_write_index_fs() throws Exception {
	final Directory directory = new FsDirectory(new File("./target/pok-index"));
	test_read_write(directory);
    }

    @Test
    public void read_and_write_index_mem() throws Exception {
	final Directory directory = new MemDirectory();
	test_read_write(directory);
    }

    @Test
    public void read_incorrect_insert_order_mem() throws Exception {
	final Directory directory = new MemDirectory();
	try (final SortedDataFileWriter<String, Byte> siw = new SortedDataFileWriter<>(
		directory,FILE_NAME, stringTd.getConvertorToBytes(), Comparator.naturalOrder(),
		byteTd.getWriter())) {
	    assertEquals(0, siw.put(new Pair<String, Byte>("aaabbb", (byte) 1)));
	    assertThrows(IllegalArgumentException.class, () -> {
		siw.put(new Pair<String, Byte>("aaa", (byte) 0), false);
	    });
	}
    }

    private void test_read_write(final Directory directory) {
	try (final SortedDataFileWriter<String, Byte> siw = new SortedDataFileWriter<>(
		directory,FILE_NAME, stringTd.getConvertorToBytes(), Comparator.naturalOrder(),
		byteTd.getWriter())) {
	    assertEquals(0, siw.put(new Pair<String, Byte>("aaa", (byte) 0), false));
	    assertEquals(6, siw.put(new Pair<String, Byte>("aaabbb", (byte) 1)));
	    assertEquals(12, siw.put(new Pair<String, Byte>("aaacc", (byte) 2)));
	    assertEquals(17, siw.put(new Pair<String, Byte>("ccc", (byte) 3)));
	}

	try (final SortedDataFileReader<String, Byte> sir = new SortedDataFileReader<>(
		directory,FILE_NAME, stringTd.getConvertorFromBytes(), byteTd.getReader(),
		Comparator.naturalOrder())) {
	    final Pair<String, Byte> p1 = sir.read();
	    final Pair<String, Byte> p2 = sir.read();
	    final Pair<String, Byte> p3 = sir.read();
	    final Pair<String, Byte> p4 = sir.read();

	    assertEquals("aaa", p1.getKey());
	    assertEquals(0, (int) p1.getValue());
	    assertEquals("aaabbb", p2.getKey());
	    assertEquals(1, (int) p2.getValue());
	    assertEquals("aaacc", p3.getKey());
	    assertEquals(2, (int) p3.getValue());
	    assertEquals("ccc", p4.getKey());
	    assertEquals(3, (int) p4.getValue());
	}
    }

    @Test
    public void test_invalidOrder() throws Exception {
	try (final SortedDataFileWriter<String, Byte> siw = new SortedDataFileWriter<>(
		new MemDirectory(),FILE_NAME, stringTd.getConvertorToBytes(), Comparator.naturalOrder(),
		byteTd.getWriter())) {
	    siw.put(new Pair<String, Byte>("aaa", (byte) 0));
	    siw.put(new Pair<String, Byte>("abbb", (byte) 1));
	    assertThrows(IllegalArgumentException.class, () -> siw.put(new Pair<String, Byte>("aaaa", (byte) 2)));
	}
    }

    @Test
    public void test_duplicatedValue() throws Exception {
	try (final SortedDataFileWriter<String, Byte> siw = new SortedDataFileWriter<>(
		new MemDirectory(),FILE_NAME, stringTd.getConvertorToBytes(), Comparator.naturalOrder(),
		byteTd.getWriter())) {
	    siw.put(new Pair<String, Byte>("aaa", (byte) 0));
	    siw.put(new Pair<String, Byte>("abbb", (byte) 1));
	    assertThrows(IllegalArgumentException.class, () -> siw.put(new Pair<String, Byte>("abbb", (byte) 2)));
	}
    }

    @Test
    public void test_null_key() throws Exception {
	try (final SortedDataFileWriter<String, Byte> siw = new SortedDataFileWriter<>(
		new MemDirectory(),FILE_NAME, stringTd.getConvertorToBytes(), Comparator.naturalOrder(),
		byteTd.getWriter())) {

	    assertThrows(NullPointerException.class, () -> siw.put(new Pair<String, Byte>(null, (byte) 0)));
	}

    }

}
