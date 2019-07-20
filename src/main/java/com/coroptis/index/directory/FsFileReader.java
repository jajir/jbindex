package com.coroptis.index.directory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import com.coroptis.index.simpleindex.IndexException;
import com.google.common.base.MoreObjects;

public class FsFileReader implements FileReader {

    private final RandomAccessFile randomAccessFile;

    FsFileReader(final File file) {
	try {
	    randomAccessFile = new RandomAccessFile(file, "r");
	} catch (IOException e) {
	    throw new IndexException(e.getMessage(), e);
	}
    }

    @Override
    public void close() {
	try {
	    randomAccessFile.close();
	} catch (IOException e) {
	    throw new IndexException(e.getMessage(), e);
	}
    }

    @Override
    public int read() {
	try {
	    return randomAccessFile.read();
	} catch (IOException e) {
	    throw new IndexException(e.getMessage(), e);
	}
    }

    @Override
    public int read(byte[] bytes) {
	try {
	    final int readBytes = randomAccessFile.read(bytes);
	    return readBytes == bytes.length ? readBytes : -1;
	} catch (IOException e) {
	    throw new IndexException(e.getMessage(), e);
	}
    }

    @Override
    public void skip(int position) {
	try {
	    randomAccessFile.seek(position);
	} catch (IOException e) {
	    throw new IndexException(e.getMessage(), e);
	}
    }

    @Override
    public String toString() {
	return MoreObjects.toStringHelper(FsFileReader.class).add("randomAccessFile", randomAccessFile.toString())
		.toString();
    }

}
