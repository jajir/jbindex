package com.coroptis.index;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;

public class SearchStringTest {

    private final int COUNT = 10;

    @Test
    public void test_search_mem() throws Exception {
	final Directory directory = new MemDirectory();
	writeIndex(directory);
	search_test(directory);
    }

    @Test
    public void test_stream() throws Exception {
	final Directory directory = new MemDirectory();
	writeIndex(directory);

	final IndexSearcher<String, String> search = new IndexSearcher<>(directory, String.class,
		String.class);
	search.getStreamer().stream().forEach(pair -> {
	    System.out.println(pair);
	    final Integer key = Integer.valueOf(pair.getKey());
	    assertTrue(key >= 0);
	    assertTrue(key < COUNT);
	    assertEquals("Ahoj", pair.getValue());
	});
    }

    private void search_test(final Directory directory) {
	final IndexSearcher<String, String> search = new IndexSearcher<>(directory, String.class,
		String.class);

	for (int i = 0; i < COUNT; i++) {
	    assertEquals("Ahoj", search.get(String.valueOf(i)));
	}

	assertNull(search.get("aaab"));
	assertNull(search.get(""));
	assertNull(search.get("aaaccc"));
	assertNull(search.get("zzzzz"));
    }

    private void writeIndex(final Directory directory) {
	try (IndexWriter<String, String> iw = new IndexWriter<>(directory, 2, String.class,
		String.class)) {
	    for (int i = 0; i < COUNT; i++) {
		iw.put(String.valueOf(i), "Ahoj");
	    }
	}
    }

}
