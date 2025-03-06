package com.coroptis.index.sorteddatafile;

/**
 * Interface definig methods for compressing and decompressing data.
 */
public interface DataCompressor {

    /**
     * Compress data.
     * 
     * @param data required data for compression
     * @return compressed data
     */
    byte[] compress(byte[] data);

    /**
     * Decompress data.
     * 
     * @param data required data for decompression
     * @return decompressed data
     */
    byte[] deCompress(byte[] data);

}
