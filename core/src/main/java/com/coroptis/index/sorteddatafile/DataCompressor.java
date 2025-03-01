package com.coroptis.index.sorteddatafile;

public interface DataCompressor {

    byte[] compress(byte[] data);

    byte[] deCompress(byte[] data);

}
