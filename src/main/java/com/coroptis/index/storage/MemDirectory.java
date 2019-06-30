package com.coroptis.index.storage;

import java.util.HashMap;
import java.util.Map;

import com.coroptis.index.simpleindex.IndexException;

public class MemDirectory implements Directory {

    private final Map<String, byte[]> data = new HashMap<>();

    @Override
    public FileReader getFileReader(final String fileName) {
	if (!data.containsKey(fileName)) {
	    throw new IndexException(String.format("There is no file '%s'", fileName));
	}
	return new MemFileReader(data.get(fileName));
    }

    @Override
    public FileWriter getFileWriter(final String fileName) {
	return new MemFileWriter(fileName, this);
    }

    void addFile(final String fileName, final byte bytes[]) {
	data.put(fileName, bytes);
    }
}
