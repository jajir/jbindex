package com.coroptis.index;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;

public class IndexMergeTest {

    final Directory directory1 = new MemDirectory();
    final Directory directory2 = new MemDirectory();
    final Directory directoryOut = new MemDirectory();

    @Test
    void test_simple() throws Exception {
	try (final IndexWriter<Integer, Integer> iw = new IndexWriter<>(directory1, 5,
		Integer.class, Integer.class)) {
	    iw.put(1, 1);
	    iw.put(2, 2);
	    iw.put(3, 3);
	    iw.put(4, 4);
	    iw.put(5, 5);
	}
	
	try (final IndexWriter<Integer, Integer> iw = new IndexWriter<>(directory2, 5,
		Integer.class, Integer.class)) {
	    iw.put(4, 4);
	    iw.put(5, 5);
	    iw.put(6, 6);
	    iw.put(7, 7);
	    iw.put(8, 8);
	}

	final IndexMerge<Integer, Integer> merger = new IndexMerge<Integer, Integer>(directory1,
		directory2, directoryOut, (kye, val1, val2) -> val1 + val2, Integer.class,
		Integer.class, 1);
	merger.merge();
	
	final IndexSearcher<Integer, Integer> search = new IndexSearcher<>(directoryOut,
		Integer.class, Integer.class);
	assertEquals(1, search.get(1));
	assertEquals(2, search.get(2));
	assertEquals(3, search.get(3));
	assertEquals(8, search.get(4));
	assertEquals(10, search.get(5));
	assertEquals(6, search.get(6));
	assertEquals(7, search.get(7));
	assertEquals(8, search.get(8));
    }

}
