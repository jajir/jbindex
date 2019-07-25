package com.coroptis.index.type;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class TypeConvertors {

    private final static TypeConvertors instance = new TypeConvertors();

    static {
	final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
	instance.add(Integer.class, ConvertorType.FROM_BYTES, tdi.getConvertorFrom());
	instance.add(Integer.class, ConvertorType.TO_BYTES, tdi.getConvertorTo());
	instance.add(Integer.class, ConvertorType.READER, tdi.getReader());
	instance.add(Integer.class, ConvertorType.WRITER, tdi.getWriter());

	final TypeDescriptorString tds = new TypeDescriptorString();
	instance.add(String.class, ConvertorType.FROM_BYTES, tds.getConvertorFromBytes());
	instance.add(String.class, ConvertorType.TO_BYTES, tds.getConvertorToBytes());
	instance.add(String.class, ConvertorType.READER, tds.getVarLengthReader());
	instance.add(String.class, ConvertorType.WRITER, tds.getVarLengthReader());
    }

    public static TypeConvertors getInstance() {
	return instance;
    }

    private final static String SEPARATOR = "-";

    private final Map<String, Object> convertors = new HashMap<>();

    public void add(final Class<?> clazz, final ConvertorType convertorType,
	    final Object convertor) {
	convertors.put(makeKey(clazz, convertorType), Objects.requireNonNull(convertor));
    }

    public <T> T get(final Class<?> clazz, final ConvertorType convertorType) {
	final T out = (T) convertors.get(makeKey(clazz, convertorType));
	return Objects.requireNonNull(out);
    }

    private String makeKey(final Class<?> clazz, final ConvertorType convertorType) {
	return clazz.getName() + SEPARATOR + convertorType.name();
    }

}
