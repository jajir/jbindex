package com.coroptis.index.directory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import com.coroptis.index.simpleindex.IndexException;

public class FsFileWriterStream implements FileWriter {

    private final OutputStream fio;
    
    private static final int BUFFER_SIZE = 1024 * 100;

    FsFileWriterStream(final File file) {
	try {
	    this.fio = new BufferedOutputStream(new FileOutputStream(file), BUFFER_SIZE);
	} catch (FileNotFoundException e) {
	    throw new IndexException(e.getMessage(), e);
	}
    }

    @Override
    public void close() {
	try {
	    fio.close();
	} catch (IOException e) {
	    throw new IndexException(e.getMessage(), e);
	}
    }

    @Override
    public void write(byte b) {
	try {
	    fio.write(b);
	} catch (IOException e) {
	    throw new IndexException(e.getMessage(), e);
	}
    }

    @Override
    public void write(byte[] bytes) {
	try {
	    fio.write(bytes);
	} catch (IOException e) {
	    throw new IndexException(e.getMessage(), e);
	}
    }

}
