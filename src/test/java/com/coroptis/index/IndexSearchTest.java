package com.coroptis.index;

import static org.junit.Assert.*;
import java.io.File;
import java.util.Comparator;

import org.junit.Test;

import com.coroptis.index.storage.Directory;
import com.coroptis.index.storage.FsDirectory;
import com.coroptis.index.storage.MemDirectory;
import com.coroptis.index.type.ByteTypeDescriptor;
import com.coroptis.index.type.StringTypeDescriptor;

public class IndexSearchTest {

    private final static String TEMP_DIR = "./target/pok-index";

    @Test
    public void test_search_mem() throws Exception {
	final Directory directory = new MemDirectory();
	writeIndex(directory);
	search_test(directory);
    }

    @Test
    public void test_search_fs() throws Exception {
	final Directory directory = new FsDirectory(new File(TEMP_DIR));
	writeIndex(directory);
	search_test(directory);
    }

    private void search_test(final Directory directory) {
	ByteTypeDescriptor byteTd = new ByteTypeDescriptor();
	StringTypeDescriptor stringTd = new StringTypeDescriptor();

	IndexSearcher<String, Byte> search = new IndexSearcher<>(directory, stringTd.getRawArrayReader(),
		Comparator.naturalOrder(), byteTd.getStreamReader());

	assertEquals(Byte.valueOf((byte) 0), search.get("aaa"));
	assertEquals(Byte.valueOf((byte) 1), search.get("aaabbb"));
	assertEquals(Byte.valueOf((byte) 2), search.get("aaacc"));
	assertEquals(Byte.valueOf((byte) 3), search.get("ccc"));

	assertNull(search.get("aaab"));
	assertNull(search.get(""));
	assertNull(search.get("aaaccc"));
	assertNull(search.get("zzzzz"));
	search.close();
    }

    private void writeIndex(final Directory directory) {
	ByteTypeDescriptor byteTd = new ByteTypeDescriptor();
	StringTypeDescriptor stringTd = new StringTypeDescriptor();

	IndexWriter<String, Byte> iw = new IndexWriter<>(directory, 2, stringTd.getRawArrayWriter(),
		Comparator.naturalOrder(), byteTd.getArrayWrite());
	iw.put("aaa", Byte.valueOf((byte) 0));
	iw.put("aaabbb", Byte.valueOf((byte) 1));
	iw.put("aaacc", Byte.valueOf((byte) 2));
	iw.put("ccc", Byte.valueOf((byte) 3));
	iw.close();
    }

}
