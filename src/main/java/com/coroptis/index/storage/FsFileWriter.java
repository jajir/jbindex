package com.coroptis.index.storage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import com.coroptis.index.simpleindex.IndexException;

public class FsFileWriter implements FileWriter {

    private final FileOutputStream fio;

    FsFileWriter(final File file) {
	try {
	    this.fio = new FileOutputStream(file);
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
