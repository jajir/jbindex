package com.coroptis.index.simpleindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.File;

import org.junit.jupiter.api.Test;

import com.coroptis.index.IndexSearcher;
import com.coroptis.index.IndexWriter;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.FsDirectory;
import com.coroptis.index.directory.MemDirectory;

public class SimpleSearchTest {

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
	IndexSearcher<String, Byte> search = new IndexSearcher<>(directory, String.class,
		Byte.class);

	assertEquals(Byte.valueOf((byte) 0), search.get("aaa"));
	assertEquals(Byte.valueOf((byte) 1), search.get("aaabbb"));
	assertEquals(Byte.valueOf((byte) 2), search.get("aaacc"));
	assertEquals(Byte.valueOf((byte) 3), search.get("ccc"));

	assertNull(search.get("aaab"));
	assertNull(search.get(""));
	assertNull(search.get("aaaccc"));
	assertNull(search.get("zzzzz"));
    }

    private void writeIndex(final Directory directory) {
	try (IndexWriter<String, Byte> iw = new IndexWriter<>(directory, 2, String.class,
		Byte.class)) {
	    iw.put("aaa", Byte.valueOf((byte) 0));
	    iw.put("aaabbb", Byte.valueOf((byte) 1));
	    iw.put("aaacc", Byte.valueOf((byte) 2));
	    iw.put("ccc", Byte.valueOf((byte) 3));
	}
    }

}
