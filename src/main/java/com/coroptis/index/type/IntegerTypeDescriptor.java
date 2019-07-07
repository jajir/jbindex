package com.coroptis.index.type;

public class IntegerTypeDescriptor {

    private TypeDescriptorInteger typeDescriptorInteger = new TypeDescriptorInteger();

    public TypeArrayWriter<Integer> getArrayWriter() {
	return object -> typeDescriptorInteger.getBytes(object);
    }

    public TypeRawArrayWriter<Integer> getRawArrayWriter() {
	return object -> typeDescriptorInteger.getBytes(object);
    }

    public TypeRawArrayReader<Integer> getRawArrayReader() {
	return bytes -> typeDescriptorInteger.load(bytes, 0);
    }

    public TypeStreamReader<Integer> getStreamReader() {
	return fileReader -> {
	    final byte[] bytes = new byte[4];
	    fileReader.read(bytes);
	    return typeDescriptorInteger.load(bytes, 0);
	};
    }

}
