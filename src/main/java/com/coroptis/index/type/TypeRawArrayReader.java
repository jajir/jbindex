package com.coroptis.index.type;

/**
 * Read object from array. Supposing that whole byte array is one object.
 *
 * @author jan
 *
 * @param <T>
 */
public interface TypeRawArrayReader<T> {

    T read(byte[] array);

}
