package com.coroptis.index.sorteddatafile;

import com.coroptis.index.directory.FileReader;

public class BufferReader implements FileReader {

    private final byte[] buffer;

    private int position;

    public BufferReader(final byte[] buffer) {
        this.buffer = buffer;
        position = 0;
    }

    @Override
    public int read() {
        if (position >= buffer.length) {
            return -1;
        }
        return buffer[position++] & 0xFF;
    }

    @Override
    public int read(final byte[] bytes) {
        if (position >= buffer.length) {
            return -1;
        }
        int bytesToRead = Math.min(bytes.length, buffer.length - position);
        System.arraycopy(buffer, position, bytes, 0, bytesToRead);
        position += bytesToRead;
        return bytesToRead;
    }

    @Override
    public void skip(final long position) {
        this.position = (int) position;
    }

    @Override
    public void close() {
        // do nothing
    }   

}
