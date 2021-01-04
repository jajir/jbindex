package com.coroptis.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;

public class SearchIntegersTest {

    private final static Integer LIMIT = 1000 * 10;

    @Test
    public void test_search_mem() throws Exception {
	final Directory directory = new MemDirectory();
	writeIndex(directory);
	search_test(directory);
    }

    private void search_test(final Directory directory) {
	IndexSearcher<Integer, Integer> search = new IndexSearcher<>(directory, Integer.class,
		Integer.class);

	for (int i = 0; i < LIMIT; i++) {
	    assertEquals(Integer.MAX_VALUE, search.get(i),
		    String.format("Key %s was not found ", i));
	}

	for (int i = 0; i < LIMIT; i++) {
	    assertNull(search.get(LIMIT + i));
	}

    }

    private void writeIndex(final Directory directory) {
	try (final IndexWriter<Integer, Integer> iw = new IndexWriter<>(directory, 5, Integer.class,
		Integer.class)) {
	    for (int i = 0; i < LIMIT; i++) {
		iw.put(i, Integer.MAX_VALUE);
	    }
	}
    }

}
