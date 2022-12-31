package com.coroptis.store;

import java.util.Random;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;
import com.coroptis.index.type.TypeDescriptorString;
import com.coroptis.index.unsorteddatafile.UnsortedDataFileStreamer;
import com.coroptis.index.unsorteddatafile.UnsortedDataFileWriter;

public class StoreStreamerTest {

    private final static int NUMBERS = 100;

    private final TypeDescriptorString td = new TypeDescriptorString();
    private final Random random = new Random();

    private Directory directory = new MemDirectory();

    @Test
    void test_read() throws Exception {
	try (final UnsortedDataFileStreamer<String, String> reader = new UnsortedDataFileStreamer<String, String>(
		directory, td.getVarLengthReader(), td.getVarLengthReader())) {
	    reader.stream().forEach(pair -> {
		System.out.println(pair.getKey());
	    });
	}
    }

    @BeforeEach
    private void setup() {
	try (final UnsortedDataFileWriter<String, String> store = new UnsortedDataFileWriter<String, String>(directory,
		String.class, String.class)) {
	    for (int i = 0; i < NUMBERS; i++) {
		store.put(String.valueOf(random.nextInt(NUMBERS)), "Ahoj");
	    }
	}
    }

}
