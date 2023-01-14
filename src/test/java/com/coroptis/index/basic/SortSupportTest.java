package com.coroptis.index.basic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.coroptis.index.directory.Directory;

public class SortSupportTest {

    @SuppressWarnings("unchecked")
    private final BasicIndex<Integer, String> index = mock(BasicIndex.class);

    @SuppressWarnings("unchecked")
    private final ValueMerger<Integer, String> valueMerger = mock(ValueMerger.class);

    private final Directory dir = mock(Directory.class);

    @Test
    void test_getFilesInRound_0() throws Exception {
	final SortSupport<Integer, String> sortSupport = makeSortSupport();

	final List<String> fileNames = sortSupport.getFilesInRound(0);
	assertEquals(5, fileNames.size());
	assertTrue(fileNames.contains("round-0-6"));
	assertTrue(fileNames.contains("round-0-7"));
	assertTrue(fileNames.contains("round-0-8"));
	assertTrue(fileNames.contains("round-0-58"));
	assertTrue(fileNames.contains("round-0-59"));
    }

    @Test
    void test_getFilesInRound_1() throws Exception {
	final SortSupport<Integer, String> sortSupport = makeSortSupport();

	final List<String> fileNames = sortSupport.getFilesInRound(1);
	assertEquals(2, fileNames.size());
	assertTrue(fileNames.contains("round-1-0"));
	assertTrue(fileNames.contains("round-1-1"));
    }

    private SortSupport<Integer, String> makeSortSupport(){
	when(index.getDirectory()).thenReturn(dir);
	final SortSupport<Integer, String> sortSupport = new SortSupport<>(index,valueMerger);
	final List<String> filesInDirectory = List.of("main.dat","round-0-6","round-0-7","round-0-8","round-0-58","round-0-59","round-1-0","round-1-1");
	when(dir.getFileNames()).thenReturn(filesInDirectory.stream());
	return sortSupport;
    }

}
