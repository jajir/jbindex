package com.coroptis.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Comparator;

import org.junit.jupiter.api.Test;

import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;
import com.coroptis.index.type.TypeDescriptorInteger;

public class SearchTest {

    private final static Integer LIMIT = 1000 * 10;

    private final TypeDescriptorInteger iTd = new TypeDescriptorInteger();

    @Test
    public void test_search_mem() throws Exception {
	final Directory directory = new MemDirectory();
	writeIndex(directory);
	search_test(directory);
    }

    private void search_test(final Directory directory) {
	IndexSearcher<Integer, Integer> search = new IndexSearcher<>(directory,
		iTd.getConvertorFrom(), Comparator.naturalOrder(), iTd.getReader());

	for (int i = 0; i < LIMIT; i++) {
	    assertEquals(Integer.MAX_VALUE, search.get(i),
		    String.format("Key %s was not found ", i));
	}

	for (int i = 0; i < LIMIT; i++) {
	    assertNull(search.get(LIMIT + i));
	}

    }

    private void writeIndex(final Directory directory) {

	final IndexWriter<Integer, Integer> iw = new IndexWriter<>(directory, 5,
		iTd.getConvertorTo(), Comparator.naturalOrder(), iTd.getConvertorTo());

	for (int i = 0; i < LIMIT; i++) {
	    iw.put(i, Integer.MAX_VALUE);
	}
	iw.close();
    }

}
