package com.coroptis.store;

import static org.mockito.Mockito.*;
import com.coroptis.index.directory.FileReader;
import com.coroptis.index.sorteddatafile.PairReader;
import com.coroptis.index.unsorteddatafile.UnsortedDataFileReader;

public class MockStoreReader extends UnsortedDataFileReader<String, String> {

    MockStoreReader(final PairReader<String, String> pairReader) {
	super(pairReader, mock(FileReader.class));
    }

}
