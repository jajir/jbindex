package com.coroptis.index.simpleindex;

import com.coroptis.index.storage.FileReader;
import com.coroptis.index.type.TypeRawArrayReader;

public class DiffKeyReader<K> {

    private final TypeRawArrayReader<K> keyTypeReader;

    private byte[] previousKeyBytes;

    public DiffKeyReader(final TypeRawArrayReader<K> keyTypeReader) {
	this.keyTypeReader = keyTypeReader;
	previousKeyBytes = new byte[0];
    }

    public K read(final FileReader fileReader) {
	final int sharedByteLength = fileReader.read();
	if (sharedByteLength == -1) {
	    return null;
	} else {
	    final int diffBytesLength = fileReader.read();
	    final byte[] diffBytes = new byte[diffBytesLength];
	    fileReader.read(diffBytes);
	    final byte[] sharedBytes = getBytes(previousKeyBytes, sharedByteLength);
	    final byte[] keyBytes = concatenateArrays(sharedBytes, diffBytes);
	    previousKeyBytes = keyBytes;

	    return keyTypeReader.read(keyBytes);
	}
    }

    private byte[] getBytes(final byte[] bytes, final int howMany) {
	final byte[] out = new byte[howMany];
	System.arraycopy(bytes, 0, out, 0, howMany);
	return out;
    }

    private byte[] concatenateArrays(final byte[] firstBytes, final byte[] secondBytes) {
	final byte[] out = new byte[firstBytes.length + secondBytes.length];
	System.arraycopy(firstBytes, 0, out, 0, firstBytes.length);
	System.arraycopy(secondBytes, 0, out, firstBytes.length, secondBytes.length);
	return out;
    }

}
