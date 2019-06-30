package com.coroptis.index.type;

import java.io.OutputStream;

public interface TypeStreamWriter<T> {

    /**
     * Write object to stream and return how many bytes was written.
     *
     * @param outputStream required output stream
     * @param object       required object to write to stream
     * @return how many bytes was written
     */
    int write(OutputStream outputStream, T object);

}
