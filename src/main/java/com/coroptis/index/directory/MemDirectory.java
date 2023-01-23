package com.coroptis.index.directory;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import com.coroptis.index.IndexException;

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

    @Override
    public void renameFile(final String currentFileName, final String newFileName) {
        if (data.containsKey(currentFileName)) {
            final byte[] tmp = data.remove(currentFileName);
            data.put(newFileName, tmp);
        }
    }

    void addFile(final String fileName, final byte bytes[]) {
        data.put(fileName, bytes);
    }

    @Override
    public boolean deleteFile(final String fileName) {
        return data.remove(fileName) != null;
    }

    @Override
    public Stream<String> getFileNames() {
        return data.keySet().stream();
    }
}
