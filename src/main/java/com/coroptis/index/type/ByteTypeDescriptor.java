package com.coroptis.index.type;

public class ByteTypeDescriptor {

    public TypeArrayWriter<Byte> getArrayWrite() {
	return b -> {
	    final byte[] out = new byte[1];
	    out[0] = b;
	    return out;
	};
    }

    public TypeStreamReader<Byte> getStreamReader() {
	return inputStream -> (byte) inputStream.read();
    }

}
