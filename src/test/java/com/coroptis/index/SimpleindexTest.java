package com.coroptis.index;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;

import org.junit.Test;

import com.coroptis.index.simpleindex.Pair;
import com.coroptis.index.simpleindex.SimpleIndexReader;
import com.coroptis.index.simpleindex.SimpleIndexWriter;
import com.coroptis.index.storage.Directory;
import com.coroptis.index.storage.FsDirectory;
import com.coroptis.index.storage.MemDirectory;
import com.coroptis.index.type.ByteTypeDescriptor;
import com.coroptis.index.type.StringTypeDescriptor;

public class SimpleindexTest {

    private final static String FILE_NAME = "pok.dat";

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

    private void test_read_write(final Directory directory) throws IOException {

	ByteTypeDescriptor byteTd = new ByteTypeDescriptor();
	StringTypeDescriptor stringTd = new StringTypeDescriptor();

	final SimpleIndexWriter<String, Byte> siw = new SimpleIndexWriter<>(directory.getFileWriter(FILE_NAME),
		stringTd.getRawArrayWriter(), Comparator.naturalOrder(), byteTd.getArrayWrite());
	assertEquals(0, siw.put(new Pair<String, Byte>("aaa", (byte) 0), false));
	assertEquals(6, siw.put(new Pair<String, Byte>("aaabbb", (byte) 1)));
	assertEquals(12, siw.put(new Pair<String, Byte>("aaacc", (byte) 2)));
	assertEquals(17, siw.put(new Pair<String, Byte>("ccc", (byte) 3)));
	siw.close();

	final SimpleIndexReader<String, Byte> sir = new SimpleIndexReader<>(directory.getFileReader(FILE_NAME),
		stringTd.getRawArrayReader(), byteTd.getStreamReader(), Comparator.naturalOrder());
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
	sir.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_invalidOrder() throws Exception {

	ByteTypeDescriptor byteTd = new ByteTypeDescriptor();
	StringTypeDescriptor stringTd = new StringTypeDescriptor();

	final SimpleIndexWriter<String, Byte> siw = new SimpleIndexWriter<>(new MemDirectory().getFileWriter(FILE_NAME),
		stringTd.getRawArrayWriter(), Comparator.naturalOrder(), byteTd.getArrayWrite());
	siw.put(new Pair<String, Byte>("aaa", (byte) 0));
	siw.put(new Pair<String, Byte>("abbb", (byte) 1));
	siw.put(new Pair<String, Byte>("aaaa", (byte) 2));
	siw.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_duplicatedValue() throws Exception {

	ByteTypeDescriptor byteTd = new ByteTypeDescriptor();
	StringTypeDescriptor stringTd = new StringTypeDescriptor();

	final SimpleIndexWriter<String, Byte> siw = new SimpleIndexWriter<>(new MemDirectory().getFileWriter(FILE_NAME),
		stringTd.getRawArrayWriter(), Comparator.naturalOrder(), byteTd.getArrayWrite());
	siw.put(new Pair<String, Byte>("aaa", (byte) 0));
	siw.put(new Pair<String, Byte>("abbb", (byte) 1));
	siw.put(new Pair<String, Byte>("abbb", (byte) 2));
	siw.close();
    }

    @Test(expected = NullPointerException.class)
    public void test_null_key() throws Exception {

	ByteTypeDescriptor byteTd = new ByteTypeDescriptor();
	StringTypeDescriptor stringTd = new StringTypeDescriptor();

	final SimpleIndexWriter<String, Byte> siw = new SimpleIndexWriter<>(new MemDirectory().getFileWriter(FILE_NAME),
		stringTd.getRawArrayWriter(), Comparator.naturalOrder(), byteTd.getArrayWrite());
	siw.put(new Pair<String, Byte>(null, (byte) 0));
	siw.close();
    }

}
