package com.coroptis.index.type;

/**
 * Write object type into byte array.
 * <p>
 * Usually first byte is type length.
 * </p>
 * 
 * @author jan
 *
 * @param <T>
 */
public interface TypeArrayWriter<T> {

    byte[] toBytes(T object);

}
