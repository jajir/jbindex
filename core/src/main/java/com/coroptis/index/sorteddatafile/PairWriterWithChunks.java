package com.coroptis.index.sorteddatafile;

import com.coroptis.index.CloseableResource;

public interface PairWriterWithChunks<K, V> extends CloseableResource {

    /**
     * Write data to next next storage and llows to chnge them or compress them.
     * @param chunk
     * @return
     */
    long writeChunk(byte[] chunk);

}
