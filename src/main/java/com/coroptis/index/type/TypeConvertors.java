package com.coroptis.index.type;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class TypeConvertors {

    private final static TypeConvertors instance = new TypeConvertors();

    static {
        {
            final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
            final Comparator<Integer> c1 = (i1, i2) -> i1 - i2;
            instance.add(Integer.class, OperationType.COMPARATOR, c1);
            instance.add(Integer.class, OperationType.CONVERTOR_FROM_BYTES, tdi.getConvertorFrom());
            instance.add(Integer.class, OperationType.CONVERTOR_TO_BYTES, tdi.getConvertorTo());
            instance.add(Integer.class, OperationType.READER, tdi.getReader());
            instance.add(Integer.class, OperationType.WRITER, tdi.getWriter());
        }
        {
            final TypeDescriptorString tds = new TypeDescriptorString();
            final Comparator<String> c2 = (s1, s2) -> s1.compareTo(s2);
            instance.add(String.class, OperationType.COMPARATOR, c2);
            instance.add(String.class, OperationType.CONVERTOR_FROM_BYTES,
                    tds.getConvertorFromBytes());
            instance.add(String.class, OperationType.CONVERTOR_TO_BYTES, tds.getConvertorToBytes());
            instance.add(String.class, OperationType.READER, tds.getVarLengthReader());
            instance.add(String.class, OperationType.WRITER, tds.getVarLenghtWriter());
        }
        {
            final TypeDescriptorByte tds = new TypeDescriptorByte();
            final Comparator<Byte> c2 = (s1, s2) -> s1 - s2;
            instance.add(Byte.class, OperationType.COMPARATOR, c2);
            instance.add(Byte.class, OperationType.CONVERTOR_FROM_BYTES,
                    tds.getConvertorFromBytes());
            instance.add(Byte.class, OperationType.CONVERTOR_TO_BYTES, tds.getConvertorToBytes());
            instance.add(Byte.class, OperationType.READER, tds.getReader());
            instance.add(Byte.class, OperationType.WRITER, tds.getWriter());
        }
        {
            final TypeDescriptorBytes tds = new TypeDescriptorBytes();
            final Comparator<byte[]> c2 = (s1, s2) -> Arrays.compare(s1, s2);
            instance.add(byte[].class, OperationType.COMPARATOR, c2);
            instance.add(byte[].class, OperationType.CONVERTOR_FROM_BYTES, tds.getConvertorFrom());
            instance.add(byte[].class, OperationType.CONVERTOR_TO_BYTES, tds.getConvertorTo());
            instance.add(byte[].class, OperationType.READER, tds.getReader());
            instance.add(byte[].class, OperationType.WRITER, tds.getWriter());
        }
    }

    public static TypeConvertors getInstance() {
        return instance;
    }

    private final static String SEPARATOR = "-";

    private final Map<String, Object> convertors = new HashMap<>();

    public void add(final Class<?> clazz, final OperationType operationType,
            final Object convertor) {
        convertors.put(makeKey(clazz, operationType), Objects.requireNonNull(convertor));
    }

    @SuppressWarnings("unchecked")
    public <T> T get(final Class<?> clazz, final OperationType operationType) {
        final String key = makeKey(clazz, operationType);
        final T out = (T) convertors.get(key);
        if (out == null) {
            throw new NullPointerException(
                    String.format("There is no %s for key %s", operationType, key));
        }
        return out;
    }

    private String makeKey(final Class<?> clazz, final OperationType operationType) {
        return clazz.getSimpleName() + SEPARATOR + operationType.name();
    }

}
