package com.coroptis.index.type;

public class LongTypeDescriptor {

    /**
     * How many bytes is required to store Integer.
     */
    private static final int REQUIRED_BYTES = 8;

    /**
     * With byte AND allows to select required part of bytes.
     */
    private static final int BYTE_MASK = 0xFF;

    // public TypeArrayWriter<Integer> getArrayWriter() {
    // return object -> typeDescriptorInteger.getBytes(object);
    // }
    //
    // public TypeRawArrayReader<Integer> getRawArrayReader() {
    // return bytes -> typeDescriptorInteger.load(bytes, 0);
    // }
    //
    // public TypeStreamReader<Integer> getStreamReader() {
    // return fileReader -> {
    // final byte[] bytes = new byte[4];
    // fileReader.read(bytes);
    // return typeDescriptorInteger.load(bytes, 0);
    // };
    // }
    //
    // private byte[] getBytes(final Integer value) {
    // int pos = 0;
    // int v = value.intValue();
    // byte[] out = new byte[REQUIRED_BYTES];
    // out[pos++] = (byte) ((v >>> BYTE_SHIFT_24) & BYTE_MASK);
    // out[pos++] = (byte) ((v >>> BYTE_SHIFT_16) & BYTE_MASK);
    // out[pos++] = (byte) ((v >>> BYTE_SHIFT_8) & BYTE_MASK);
    // out[pos] = (byte) ((v >>> BYTE_SHIFT_0) & BYTE_MASK);
    // return out;
    // }

}
