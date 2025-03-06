package com.coroptis.index.sorteddatafile;

import com.coroptis.index.CloseableResource;

public interface SeekeableFileWriter extends CloseableResource {

    void write(byte[] bytes);

    void write(byte b);

    long flushAndWrite(byte[] bytes);

}
