package com.coroptis.index.type;

import java.nio.charset.Charset;

public class StringTypeDescriptor {

    private final static String CHARSET_ENCODING_NAME = "ISO_8859_1";

    private final static Charset CHARSET_ENCODING = Charset.forName(CHARSET_ENCODING_NAME);

    public TypeRawArrayReader<String> getRawArrayReader() {
	return array -> new String(array, CHARSET_ENCODING);
    }

    public TypeRawArrayWriter<String> getRawArrayWriter() {
	return object -> object.getBytes(CHARSET_ENCODING);
    }

    public TypeArrayWriter<String> getArrayWriter() {
	return object -> object.getBytes(CHARSET_ENCODING);
    }

}
