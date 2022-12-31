package com.coroptis.store;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.coroptis.index.IndexSearcher;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;
import com.coroptis.index.unsorteddatafile.StoreSorter;
import com.coroptis.index.unsorteddatafile.UnsortedDataFileWriter;

public class SortTest {

    private Directory directory = new MemDirectory();

    private final int BLOCK_SIZE = 2;

    @Test
    void test_empty() throws Exception {
	setup(Arrays.asList());

	final StoreSorter<String, String> sorter = new StoreSorter<String, String>(directory,
		(key, value1, value2) -> value1, String.class, String.class, 1, BLOCK_SIZE);
	sorter.sort();

	verifyIndex(Arrays.asList(),
		Arrays.asList("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"));
	assertEquals(3, directory.getFileNames().count());
    }

    @Test
    void test_basic() throws Exception {
	setup(Arrays.asList("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"));

	final StoreSorter<String, String> sorter = new StoreSorter<String, String>(directory,
		(key, value1, value2) -> value1, String.class, String.class, 1, BLOCK_SIZE);
	sorter.sort();

	verifyIndex(Arrays.asList("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"));
	assertEquals(3, directory.getFileNames().count());
    }

    @Test
    void test_random() throws Exception {
	setup(Arrays.asList("9", "5", "3", "1", "4", "2", "6", "7", "8", "0"));

	final StoreSorter<String, String> sorter = new StoreSorter<String, String>(directory,
		(key, value1, value2) -> value1, String.class, String.class, 1, BLOCK_SIZE);
	sorter.sort();

	verifyIndex(Arrays.asList("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"));
	assertEquals(3, directory.getFileNames().count());
    }

    @Test
    void test_invers_order() throws Exception {
	setup(Arrays.asList("9", "8", "7", "6", "5", "4", "3", "2", "1", "0"));

	final StoreSorter<String, String> sorter = new StoreSorter<String, String>(directory,
		(key, value1, value2) -> value1, String.class, String.class, 1, BLOCK_SIZE);
	sorter.sort();

	verifyIndex(Arrays.asList("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"));
	assertEquals(3, directory.getFileNames().count());
    }

    @Test
    void test_invers_order_sort_in_mem() throws Exception {
	setup(Arrays.asList("9", "8", "7", "6", "5", "4", "3", "2", "1", "0"));

	final StoreSorter<String, String> sorter = new StoreSorter<String, String>(directory,
		(key, value1, value2) -> value1, String.class, String.class, 100, BLOCK_SIZE);
	sorter.sort();

	verifyIndex(Arrays.asList("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"));
	assertEquals(3, directory.getFileNames().count());
    }

    @Test
    void test_same_values() throws Exception {
	setup(Arrays.asList("0", "0", "0", "0", "0", "0", "0", "0", "0", "0"));

	final StoreSorter<String, String> sorter = new StoreSorter<String, String>(directory,
		(key, value1, value2) -> value1, String.class, String.class, 1, BLOCK_SIZE);
	sorter.sort();

	verifyIndex(Arrays.asList("0"), Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9"));
	assertEquals(3, directory.getFileNames().count());
    }

    @Test
    void test_same_values_3() throws Exception {
	setup(Arrays.asList("0", "0", "0", "0", "0", "0", "0", "0", "0", "0"));

	final StoreSorter<String, String> sorter = new StoreSorter<String, String>(directory,
		(key, value1, value2) -> value1, String.class, String.class, 3, BLOCK_SIZE);
	sorter.sort();

	verifyIndex(Arrays.asList("0"), Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9"));
	assertEquals(3, directory.getFileNames().count());
    }

    private void verifyIndex(final List<String> keys) {
	verifyIndex(keys, Arrays.asList("1000"));
    }

    private void verifyIndex(final List<String> containinKeys,
	    final List<String> notContainingKeys) {
	final IndexSearcher<String, String> searcher = new IndexSearcher<String, String>(directory,
		String.class, String.class);
	containinKeys.forEach(key -> {
	    assertEquals("Ahoj", searcher.get(key));
	});
	notContainingKeys.forEach(key -> {
	    assertNull(searcher.get(key));
	});
    }

    private void setup(final List<String> keys) {
	try (final UnsortedDataFileWriter<String, String> store = new UnsortedDataFileWriter<String, String>(directory,
		String.class, String.class)) {
	    keys.forEach(key -> store.put(key, "Ahoj"));
	}
    }

}
