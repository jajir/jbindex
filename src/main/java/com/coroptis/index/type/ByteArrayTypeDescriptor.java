package com.coroptis.index.type;

public class ByteArrayTypeDescriptor {

    public TypeRawArrayReader<byte[]> getRawArrayReader() {
	return array -> array;
    }

    public TypeRawArrayWriter<byte[]> getRawArrayWriter() {
	return object -> object;
    }

    public TypeArrayWriter<byte[]> getArrayWriter() {
	return object -> object;
    }

    public TypeStreamReader<byte[]> getStreamReader() {
	return fileReader -> {
	    final byte[] bytes = new byte[2];
	    fileReader.read(bytes);
	    return bytes;
	};
    }

}
