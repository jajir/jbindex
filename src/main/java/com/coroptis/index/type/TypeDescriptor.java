package com.coroptis.index.type;

import java.util.Comparator;

/**
 * Describe index type. Depending on usage of type some methods doesn't have to
 * be implemented. When type is used as value type than sorting will not be
 * used.
 * 
 * @author honza
 *
 * @param <T> Described type
 */
public interface TypeDescriptor<T> {

    /**
     * Comparator of value.
     * 
     * @return
     */
    Comparator<T> getComparator();

    TypeReader<T> getTypeReader();

    TypeWriter<T> getTypeWriter();

    ConvertorFromBytes<T> getConvertorFromBytes();

    ConvertorToBytes<T> getConvertorToBytes();

}
