package com.coroptis.index.type;

/**
 * Write object to byte array. Supposing that whole byte array is one object.
 * 
 * @author jan
 *
 * @param <T>
 */
public interface TypeRawArrayWriter<T> {

    byte[] toBytes(T object);

}
