package com.coroptis.index.unsorteddatafile;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;

public class UnsortedFileTest {

    @Test
    public void test_in_mem_unsorted_index() throws Exception {
	final Directory dir = new MemDirectory();
	final String fileName = "duck";

	final UnsortedDataFile<Integer, String> unsortedFile = UnsortedDataFile.<Integer, String>builder().withDirectory(dir)
		.withFile(fileName).build();

	assertNotNull(unsortedFile);
    }

}
