package com.coroptis.index.simpleindex;

import java.io.Closeable;

/**
 * Extends {@link Closeable} interface and remove IOException.
 * 
 * @author jan
 *
 */
public interface CloseableResource extends Closeable {

    @Override
    public void close();

}
