package com.coroptis.store;

import static org.mockito.Mockito.*;
import com.coroptis.index.directory.FileReader;
import com.coroptis.index.simpleindex.PairReader;

public class MockStoreReader extends StoreReader<String, String> {

    MockStoreReader(final PairReader<String, String> pairReader) {
	super(pairReader, mock(FileReader.class));
    }

}
