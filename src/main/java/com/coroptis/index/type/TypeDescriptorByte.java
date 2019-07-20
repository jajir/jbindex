package com.coroptis.index.type;

public class TypeDescriptorByte {

    public ConvertorToBytes<Byte> getConvertorToBytes() {
	return b -> {
	    final byte[] out = new byte[1];
	    out[0] = b;
	    return out;
	};
    }

    public TypeReader<Byte> getReader() {
	return inputStream -> (byte) inputStream.read();
    }

}
