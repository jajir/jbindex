package com.coroptis.index.storage;

public class MemFileReader implements FileReader {

    private final byte[] data;

    private int position;

    MemFileReader(final byte[] data) {
	this.data = data;
	position = 0;
    }

    @Override
    public void close() {
	position = -1;
    }

    @Override
    public int read() {
	if (position < data.length) {
	    return data[position++];
	} else {
	    return -1;
	}
    }

    @Override
    public int read(byte[] bytes) {
	final int newPosition = position + bytes.length;
	if (newPosition <= data.length) {
	    System.arraycopy(data, position, bytes, 0, bytes.length);
	    position = newPosition;
	    return bytes.length;
	} else {
	    return -1;
	}
    }

    @Override
    public void seek(int position) {
	this.position = position;
    }
}
