package com.coroptis.index.type;

public class TypeDescriptorInteger {

    /**
     * How many bytes is required to store Integer.
     */
    private static final int REQUIRED_BYTES = 4;

    /**
     * With byte AND allows to select required part of bytes.
     */
    private static final int BYTE_MASK = 0xFF;

    /**
     * Bite shift for 0 bits.
     */
    private static final int BYTE_SHIFT_0 = 0;

    /**
     * Bite shift for 8 bits.
     */
    private static final int BYTE_SHIFT_8 = 8;

    /**
     * Bite shift for 16 bits.
     */
    private static final int BYTE_SHIFT_16 = 16;

    /**
     * Bite shift for 24 bits.
     */
    private static final int BYTE_SHIFT_24 = 24;

    public ConvertorToBytes<Integer> getConvertorTo() {
        return object -> getBytes(object);
    }

    public ConvertorFromBytes<Integer> getConvertorFrom() {
        return bytes -> load(bytes, 0);
    }

    public TypeReader<Integer> getReader() {
        return fileReader -> {
            final byte[] bytes = new byte[4];
            if (fileReader.read(bytes) == -1) {
                return null;
            }
            return load(bytes, 0);
        };
    }

    public TypeWriter<Integer> getWriter() {
        return (writer, object) -> {
            writer.write(getBytes(object));
            return 4;
        };
    }

    private byte[] getBytes(final Integer value) {
        int pos = 0;
        int v = value.intValue();
        byte[] out = new byte[REQUIRED_BYTES];
        out[pos++] = (byte) ((v >>> BYTE_SHIFT_24) & BYTE_MASK);
        out[pos++] = (byte) ((v >>> BYTE_SHIFT_16) & BYTE_MASK);
        out[pos++] = (byte) ((v >>> BYTE_SHIFT_8) & BYTE_MASK);
        out[pos] = (byte) ((v >>> BYTE_SHIFT_0) & BYTE_MASK);
        return out;
    }

    private Integer load(final byte[] data, final int from) {
        int pos = from;
        return data[pos++] << BYTE_SHIFT_24 | (data[pos++] & BYTE_MASK) << BYTE_SHIFT_16
                | (data[pos++] & BYTE_MASK) << BYTE_SHIFT_8 | (data[pos] & BYTE_MASK);
    }

}
