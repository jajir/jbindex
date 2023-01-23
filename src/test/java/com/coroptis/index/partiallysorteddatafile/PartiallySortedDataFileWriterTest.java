package com.coroptis.index.partiallysorteddatafile;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Comparator;

import org.junit.jupiter.api.Test;

import com.coroptis.index.basic.BasicIndex;
import com.coroptis.index.basic.DefaultValueMerger;
import com.coroptis.index.sorteddatafile.Pair;
import com.coroptis.index.sorteddatafile.SortedDataFile;
import com.coroptis.index.sorteddatafile.SortedDataFileWriter;

public class PartiallySortedDataFileWriterTest {

    @SuppressWarnings("unchecked")
    private final BasicIndex<Integer, String> basicIndex = mock(BasicIndex.class);

    @SuppressWarnings("unchecked")
    private final SortedDataFile<Integer, String> sortedDataFile1 = mock(SortedDataFile.class);

    @SuppressWarnings("unchecked")
    private final SortedDataFileWriter<Integer, String> partialWriter1 = mock(SortedDataFileWriter.class);

    @SuppressWarnings("unchecked")
    private final SortedDataFile<Integer, String> sortedDataFile2 = mock(SortedDataFile.class);

    @SuppressWarnings("unchecked")
    private final SortedDataFileWriter<Integer, String> partialWriter2 = mock(SortedDataFileWriter.class);

    private final PartiallySortedDataFileWriter<Integer, String> writer = new PartiallySortedDataFileWriter<>(
	    "datafile", new DefaultValueMerger<>(), 2, basicIndex, Comparator.naturalOrder());

    @Test
    void test_put_1values() throws Exception {
	writer.put(Pair.of(9, "world"));

	when(basicIndex.getSortedDataFile("datafile-0-0")).thenReturn(sortedDataFile1);
	when(sortedDataFile1.openWriter()).thenReturn(partialWriter1);
	writer.close();

	verify(partialWriter1, times(1)).put(Pair.of(9, "world"));
	verify(partialWriter1, times(1)).close();
	
    }

    @Test
    void test_put_2values() throws Exception {
	writer.put(Pair.of(9, "world"));
	when(basicIndex.getSortedDataFile("datafile-0-0")).thenReturn(sortedDataFile1);
	when(sortedDataFile1.openWriter()).thenReturn(partialWriter1);
	writer.put(Pair.of(2, "Hello"));
	verify(partialWriter1, times(1)).put(Pair.of(2, "Hello"));
	verify(partialWriter1, times(1)).put(Pair.of(9, "world"));
	verify(partialWriter1, times(1)).close();

	writer.close();
    }

    @Test
    void test_put_3values() throws Exception {
	writer.put(Pair.of(9, "world"));
	when(basicIndex.getSortedDataFile("datafile-0-0")).thenReturn(sortedDataFile1);
	when(sortedDataFile1.openWriter()).thenReturn(partialWriter1);
	writer.put(Pair.of(2, "Hello"));
	verify(partialWriter1, times(1)).put(Pair.of(2, "Hello"));
	verify(partialWriter1, times(1)).put(Pair.of(9, "world"));
	verify(partialWriter1, times(1)).close();

	writer.put(Pair.of(1, "Cult"));
	when(basicIndex.getSortedDataFile("datafile-0-1")).thenReturn(sortedDataFile2);
	when(sortedDataFile2.openWriter()).thenReturn(partialWriter2);
	
	writer.close();
	verify(partialWriter2, times(1)).put(Pair.of(1, "Cult"));
	verify(partialWriter2, times(1)).close();
    }

}
