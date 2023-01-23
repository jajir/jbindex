package com.coroptis.index.type;

public class TypeDescriptorBytes {

    public ConvertorToBytes<byte[]> getConvertorTo() {
        return object -> getBytes(object);
    }

    public ConvertorFromBytes<byte[]> getConvertorFrom() {
        return bytes -> load(bytes, 0);
    }

    public TypeReader<byte[]> getReader() {
        return fileReader -> {
            final byte[] bytes = new byte[4];
            fileReader.read(bytes);
            return load(bytes, 0);
        };
    }

    public TypeWriter<byte[]> getWriter() {
        return (writer, object) -> {
            writer.write(getBytes(object));
            return 4;
        };
    }

    private byte[] getBytes(final byte[] value) {
        return value;
    }

    private byte[] load(final byte[] data, final int from) {
        int len = data.length - from;
        byte[] out = new byte[len];
        System.arraycopy(data, from, data, 0, len);
        return out;
    }

}
