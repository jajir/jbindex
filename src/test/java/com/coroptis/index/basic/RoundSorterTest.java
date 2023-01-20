package com.coroptis.index.basic;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.Test;

public class RoundSorterTest {

    @SuppressWarnings("unchecked")
    private final SortSupport<Integer, String> sortSupport = mock(SortSupport.class);

    @SuppressWarnings("unchecked")
    private final BasicIndex<Integer, String> basicIndex = mock(BasicIndex.class);

    /**
     * Verify from file in directory correct number of files is merged.
     * 
     * @throws Exception
     */
    @Test
    void test_mergeRound() throws Exception {
	final RoundSorted<Integer, String> roundSorted = new RoundSorted<>(basicIndex, sortSupport, 3);
	final List<String> filesInRound0 = List.of("round-0-0", "round-0-1", "round-0-2", "round-0-3", "round-0-4");

	when(sortSupport.getFilesInRound(0)).thenReturn(filesInRound0);
	when(sortSupport.makeFileName(1, 0)).thenReturn("round-0-0");
	when(sortSupport.makeFileName(1, 1)).thenReturn("round-0-1");

	// real method call
	roundSorted.mergeRound(0, pair -> System.out.println(pair));

	final List<String> filesToMerge0 = List.of("round-0-0", "round-0-1", "round-0-2");
	final List<String> filesToMerge1 = List.of("round-0-3", "round-0-4");

	verify(sortSupport).mergeSortedFiles(filesToMerge0, "round-0-0");
	verify(sortSupport).mergeSortedFiles(filesToMerge1, "round-0-1");

	verify(basicIndex).deleteFile("round-0-0");
	verify(basicIndex).deleteFile("round-0-1");
	verify(basicIndex).deleteFile("round-0-2");
	verify(basicIndex).deleteFile("round-0-3");
	verify(basicIndex).deleteFile("round-0-4");
    }

}
