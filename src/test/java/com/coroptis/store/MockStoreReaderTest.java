package com.coroptis.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

import com.coroptis.index.directory.FileReader;
import com.coroptis.index.sorteddatafile.Pair;
import com.coroptis.index.sorteddatafile.PairReader;

public class MockStoreReaderTest {

    @SuppressWarnings("unchecked")
    @Test
    void testBasic() throws Exception {
	final PairReader<String, String> pairReader = mock(PairReader.class);
	when(pairReader.read(any(FileReader.class))).thenReturn(new Pair<String, String>("b", "b"),
		(Pair<String, String>[]) null);

	MockStoreReader reader = new MockStoreReader(pairReader);
	assertTrue(reader.readCurrent().isPresent());
	assertEquals("b", reader.readCurrent().get().getKey());
	assertEquals("b", reader.readCurrent().get().getKey());
	assertEquals("b", reader.readCurrent().get().getKey());
	assertEquals("b", reader.readCurrent().get().getKey());

	reader.moveToNext();
	assertFalse(reader.readCurrent().isPresent());
    }

}
