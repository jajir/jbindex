package com.coroptis.index.fastindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.coroptis.index.Pair;
import com.coroptis.index.PairFileReader;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;
import com.coroptis.index.type.TypeDescriptorInteger;
import com.coroptis.index.type.TypeDescriptorString;

public class FastIndexTest {

    @Test
    public void test_simple() throws Exception {
	final Directory directory = new MemDirectory();
	final FastIndex<Integer, String> fastIndex = makeIndex(directory);

	fastIndex.put(Pair.of(0, "Miluje"));
	fastIndex.put(Pair.of(1, "psaní"));
	fastIndex.close();

	assertEquals(3, directory.getFileNames().count());
	assertTrue(directory.isFileExists("index.map"));
	assertTrue(directory.isFileExists("segment-00000.unsorted"));
	assertTrue(directory.isFileExists("segment-00000.properties"));

	try (final PairFileReader<Integer, String> reader = fastIndex.openReader()) {
	    assertEquals(Pair.of(0, "Miluje"), reader.read());
	    assertEquals(Pair.of(1, "psaní"), reader.read());
	    assertNull(reader.read());
	}
    }

    @Test
    public void test_with_compacting() throws Exception {
	final Directory directory = new MemDirectory();
	final FastIndex<Integer, String> fastIndex = makeIndex(directory);

	fastIndex.put(Pair.of(0, "Miluje"));
	fastIndex.put(Pair.of(1, "psaní"));
	fastIndex.put(Pair.of(2, "a"));
	fastIndex.put(Pair.of(3, "vse"));
	fastIndex.put(Pair.of(4, "co"));
	fastIndex.put(Pair.of(5, "se"));
	fastIndex.put(Pair.of(6, "k"));
	fastIndex.put(Pair.of(7, "nemu"));
	fastIndex.put(Pair.of(8, "vaze"));
	fastIndex.put(Pair.of(9, "Pise"));
	fastIndex.put(Pair.of(10, "povidky"));
	fastIndex.close();

	assertEquals(6, directory.getFileNames().count());
	assertTrue(directory.isFileExists("index.map"));
	assertTrue(directory.isFileExists("segment-00000.sorted"));
	assertTrue(directory.isFileExists("segment-00000.properties"));
	assertTrue(directory.isFileExists("segment-00000.unsorted"));
	assertTrue(directory.isFileExists("segment-00001.sorted"));
	assertTrue(directory.isFileExists("segment-00001.properties"));

	try (final PairFileReader<Integer, String> reader = fastIndex.openReader()) {
	    assertEquals(Pair.of(0, "Miluje"), reader.read());
	    assertEquals(Pair.of(1, "psaní"), reader.read());
	    assertEquals(Pair.of(2, "a"), reader.read());
	    assertEquals(Pair.of(3, "vse"), reader.read());
	    assertEquals(Pair.of(4, "co"), reader.read());
	    assertEquals(Pair.of(5, "se"), reader.read());
	    assertEquals(Pair.of(6, "k"), reader.read());
	    assertEquals(Pair.of(7, "nemu"), reader.read());
	    assertEquals(Pair.of(8, "vaze"), reader.read());
	    assertEquals(Pair.of(9, "Pise"), reader.read());
	    assertEquals(Pair.of(10, "povidky"), reader.read());
	    assertNull(reader.read());
	}
    }

    private FastIndex<Integer, String> makeIndex(final Directory directory) {
	final FastIndex<Integer, String> fastIndex = FastIndex.<Integer, String>builder().withDirectory(directory)
		.withKeyTypeDescriptor(new TypeDescriptorInteger()).withValueTypeDescriptor(new TypeDescriptorString())
		.withValueMerger((k, v1, v2) -> v1 + v2).withMaxNumberOfKeysInCache(2).withMaxNumeberOfKeysInSegment(5)
		.withMaxNumeberOfKeysInSegmentCache(3).build();
	return fastIndex;
    }

}
