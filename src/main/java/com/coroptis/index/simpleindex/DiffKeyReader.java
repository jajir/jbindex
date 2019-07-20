package com.coroptis.index.simpleindex;

import com.coroptis.index.directory.FileReader;
import com.coroptis.index.type.ConvertorFromBytes;
import com.coroptis.index.type.TypeReader;

public class DiffKeyReader<K> implements TypeReader<K> {

    private final ConvertorFromBytes<K> keyConvertor;

    private byte[] previousKeyBytes;

    public DiffKeyReader(final ConvertorFromBytes<K> keyConvertor) {
	this.keyConvertor = keyConvertor;
	previousKeyBytes = new byte[0];
    }

    @Override
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

	    return keyConvertor.fromBytes(keyBytes);
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
