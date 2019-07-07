package com.coroptis.index.type;

/**
 * Allows to read object from byte array without knowing exact type length.
 * <p>
 * Usually first byte is type length.
 * </p>
 * 
 * @author jan
 *
 * @param <T>
 */
public interface TypeArrayReader<T> {

    T read(byte[] array);

}
