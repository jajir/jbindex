package com.coroptis.index.directory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import com.coroptis.index.IndexException;
import com.google.common.base.MoreObjects;

public class FsFileReaderStream implements FileReader {

    private final BufferedInputStream bis;

    private final static int BUFFER_SIZE = 1024 * 1024 * 5;

    FsFileReaderStream(final File file) {
	try {
	    FileInputStream fin = new FileInputStream(file);
	    bis = new BufferedInputStream(fin, BUFFER_SIZE);
	} catch (IOException e) {
	    throw new IndexException(e.getMessage(), e);
	}
    }

    @Override
    public void close() {
	try {
	    bis.close();
	} catch (IOException e) {
	    throw new IndexException(e.getMessage(), e);
	}
    }

    @Override
    public int read() {
	try {
	    return bis.read();
	} catch (IOException e) {
	    throw new IndexException(e.getMessage(), e);
	}
    }

    @Override
    public int read(byte[] bytes) {
	try {
	    final int readBytes = bis.read(bytes);
	    return readBytes == bytes.length ? readBytes : -1;
	} catch (IOException e) {
	    throw new IndexException(e.getMessage(), e);
	}
    }

    @Override
    public void skip(int position) {
	try {
	    bis.skip(position);
	} catch (IOException e) {
	    throw new IndexException(e.getMessage(), e);
	}
    }

    @Override
    public String toString() {
	return MoreObjects.toStringHelper(FsFileReaderStream.class).add("bis", bis.toString()).toString();
    }

}
