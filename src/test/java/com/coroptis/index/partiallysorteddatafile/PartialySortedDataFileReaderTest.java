package com.coroptis.index.partiallysorteddatafile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

import com.coroptis.index.DataFileReaderImpl;
import com.coroptis.index.Pair;
import com.coroptis.index.basic.BasicIndex;
import com.coroptis.index.basic.SortSupport;
import com.coroptis.index.sorteddatafile.SortedDataFile;

public class PartialySortedDataFileReaderTest {

    @SuppressWarnings("unchecked")
    private final BasicIndex<Integer, String> basicIndex = mock(BasicIndex.class);

    @SuppressWarnings("unchecked")
    private final SortSupport<Integer, String> sortSupport = mock(SortSupport.class);

    @SuppressWarnings("unchecked")
    private final SortedDataFile<Integer, String> dataFile0 = mock(SortedDataFile.class);

    @SuppressWarnings("unchecked")
    private final DataFileReaderImpl<Integer, String> dataFileReader0 = mock(DataFileReaderImpl.class);

    @SuppressWarnings("unchecked")
    private final SortedDataFile<Integer, String> dataFile1 = mock(SortedDataFile.class);

    @SuppressWarnings("unchecked")
    private final DataFileReaderImpl<Integer, String> dataFileReader1 = mock(DataFileReaderImpl.class);

    final String[] files = new String[] { "file-0", "file-1" };

    /*
     * Verify that reader correctly read data between files.
     *
     */
    @Test
    void test_complex_reading() throws Exception {
	when(sortSupport.getFilesInRound(0)).thenReturn(new ArrayList<String>(Arrays.asList(files)));
	
	// prepare index file 0
	when(basicIndex.getSortedDataFile("file-0")).thenReturn(dataFile0);
	when(dataFile0.openReader()).thenReturn(dataFileReader0);
	try(final PartiallySortedDataFileReader<Integer, String> reader = new PartiallySortedDataFileReader<Integer, String>(
		basicIndex, sortSupport)){
	when(dataFileReader0.read()).thenReturn(Pair.of(4, "duck"));
	assertEquals(Pair.of(4, "duck"), reader.read());
	
	//end of file 0
	when(dataFileReader0.read()).thenReturn(null);
	
	// prepare index file 1
	when(basicIndex.getSortedDataFile("file-1")).thenReturn(dataFile1);
	when(dataFile1.openReader()).thenReturn(dataFileReader1);
	when(dataFileReader1.read()).thenReturn(Pair.of(1, "is"));
	assertEquals(Pair.of(1, "is"), reader.read());
	
	when(dataFileReader1.read()).thenReturn(null);
	assertNull(reader.read());}
    }

    @Test
    void test_complex_reading_noFiles() throws Exception {
	when(sortSupport.getFilesInRound(0)).thenReturn(new ArrayList<String>());
	
	// prepare index file 0
	try(final PartiallySortedDataFileReader<Integer, String> reader = new PartiallySortedDataFileReader<Integer, String>(
		basicIndex, sortSupport)){
	assertNull(reader.read());
	assertNull(reader.read());
	assertNull(reader.read());
	}
    }

}
