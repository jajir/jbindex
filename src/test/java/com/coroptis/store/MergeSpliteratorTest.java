package com.coroptis.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

import com.coroptis.index.directory.FileReader;
import com.coroptis.index.sorteddatafile.Pair;
import com.coroptis.index.sorteddatafile.PairReader;
import com.coroptis.index.unsorteddatafile.MergeSpliterator;

public class MergeSpliteratorTest {

    @SuppressWarnings("unchecked")
    final PairReader<String, String> pairReader1 = mock(PairReader.class);

    @SuppressWarnings("unchecked")
    final PairReader<String, String> pairReader2 = mock(PairReader.class);

    @Test
    void test_1_valid_in_reaeder_1() throws Exception {
	when(pairReader1.read(any(FileReader.class))).thenReturn(new Pair<String, String>("a", "a"),
		(Pair<String, String>[]) null);
	final MockStoreReader reader1 = new MockStoreReader(pairReader1);

	when(pairReader2.read(any(FileReader.class))).thenReturn((Pair<String, String>) null);
	final MockStoreReader reader2 = new MockStoreReader(pairReader2);

	final MergeSpliterator<String, String> pok = new MergeSpliterator<String, String>(reader1,
		reader2, (str1, str2) -> str1.compareTo(str2), (key, val1, val2) -> val1);

	assertTrue(pok.tryAdvance(pair -> {
	    assertEquals("a", pair.getKey());
	}));
	assertFalse(pok.tryAdvance(pair -> {
	    fail();
	}));
    }

    @Test
    void test_1_valid_in_reaeder_2() throws Exception {
	when(pairReader1.read(any(FileReader.class))).thenReturn((Pair<String, String>) null);
	final MockStoreReader reader1 = new MockStoreReader(pairReader1);

	when(pairReader2.read(any(FileReader.class))).thenReturn(new Pair<String, String>("a", "a"),
		(Pair<String, String>[]) null);
	final MockStoreReader reader2 = new MockStoreReader(pairReader2);

	final MergeSpliterator<String, String> pok = new MergeSpliterator<String, String>(reader1,
		reader2, (str1, str2) -> str1.compareTo(str2), (key, val1, val2) -> val1);

	assertTrue(pok.tryAdvance(pair -> {
	    assertEquals("a", pair.getKey());
	}));
	assertFalse(pok.tryAdvance(pair -> {
	    fail();
	}));
    }

    @Test
    void test_two_values() throws Exception {
	when(pairReader1.read(any(FileReader.class))).thenReturn(new Pair<String, String>("a", "a"),
		(Pair<String, String>[]) null);
	final MockStoreReader reader1 = new MockStoreReader(pairReader1);

	when(pairReader2.read(any(FileReader.class))).thenReturn(new Pair<String, String>("b", "b"),
		(Pair<String, String>[]) null);
	final MockStoreReader reader2 = new MockStoreReader(pairReader2);

	final MergeSpliterator<String, String> pok = new MergeSpliterator<String, String>(reader1,
		reader2, (str1, str2) -> str1.compareTo(str2), (key, val1, val2) -> val1);

	assertTrue(pok.tryAdvance(pair -> {
	    assertEquals("a", pair.getKey());
	}));
	assertTrue(pok.tryAdvance(pair -> {
	    assertEquals("b", pair.getKey());
	}));
	assertFalse(pok.tryAdvance(pair -> {
	    fail();
	}));
    }
}
