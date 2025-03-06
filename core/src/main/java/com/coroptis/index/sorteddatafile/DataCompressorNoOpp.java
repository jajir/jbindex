package com.coroptis.index.sorteddatafile;

/**
 * Data compressor that doesn't compress nothing at all. It's default
 * implementation.
 */
public class DataCompressorNoOpp implements DataCompressor {

    @Override
    public byte[] compress(byte[] data) {
        return data;
    }

    @Override
    public byte[] deCompress(byte[] data) {
        return data;
    }

}
