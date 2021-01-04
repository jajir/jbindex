package com.coroptis.index.type;

import java.nio.charset.Charset;

public class TypeDescriptorString {

    private final static String CHARSET_ENCODING_NAME = "ISO_8859_1";

    private final static Charset CHARSET_ENCODING = Charset.forName(CHARSET_ENCODING_NAME);

    public ConvertorFromBytes<String> getConvertorFromBytes() {
	return array -> new String(array, CHARSET_ENCODING);
    }

    public ConvertorToBytes<String> getConvertorToBytes() {
	return string -> string.getBytes(CHARSET_ENCODING);
    }

    public VarLengthWriter<String> getVarLenghtWriter() {
	return new VarLengthWriter<String>(getConvertorToBytes());
    }

    public VarLengthReader<String> getVarLengthReader() {
	return new VarLengthReader<String>(getConvertorFromBytes());
    }
}
