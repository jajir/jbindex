package com.coroptis.index.simpleindex;

import java.util.Comparator;
import java.util.Objects;

import com.coroptis.index.storage.FileWriter;
import com.coroptis.index.type.TypeRawArrayWriter;

public class DiffKeyWriter<K> {

    private final TypeRawArrayWriter<K> keyTypeWriter;

    private final Comparator<? super K> keyComparator;

    private byte[] previousKeyBytes;

    private K previousKey;

    public DiffKeyWriter(final TypeRawArrayWriter<K> keyTypeRawArrayWriter,
	    final Comparator<? super K> keyComparator) {
	this.keyTypeWriter = keyTypeRawArrayWriter;
	this.keyComparator = Objects.requireNonNull(keyComparator, "Key comparator can't be null");
	previousKeyBytes = new byte[0];
	previousKey = null;
    }

    public int write(final FileWriter fileWriter, final K key, final boolean fullWrite) {
	Objects.requireNonNull(key, "key can't be null");
	if (previousKey != null) {
	    final int cmp = keyComparator.compare(previousKey, key);
	    if (cmp == 0) {
		throw new IllegalArgumentException(
			String.format("Attempt to insers same key as previous. Key is %s.", key));
	    }
	    if (cmp > 0) {
		throw new IllegalArgumentException(String.format(
			"Attempt to insers key in invalid order. Previous key is %s, inserted key is %s",
			previousKey, key));
	    }
	}
	if (fullWrite) {
	    final byte[] keyBytes = keyTypeWriter.toBytes(key);

	    return write(fileWriter, 0, keyBytes, key, keyBytes);
	} else {
	    final byte[] keyBytes = keyTypeWriter.toBytes(key);
	    final int sharedByteLength = howMuchIsSame(previousKeyBytes, keyBytes);
	    final byte[] diffBytes = getDiffPart(sharedByteLength, keyBytes);

	    return write(fileWriter, sharedByteLength, diffBytes, key, keyBytes);
	}
    }

    private int write(final FileWriter fileWriter, final int sharedByteLength,
	    final byte[] diffBytes, final K key, final byte[] keyBytes) {
	fileWriter.write((byte) (sharedByteLength));
	fileWriter.write((byte) (diffBytes.length));
	fileWriter.write(diffBytes);

	previousKeyBytes = keyBytes;
	previousKey = key;
	return 2 + diffBytes.length;
    }

    int howMuchIsSame(final byte[] previousBytes, final byte[] currentBytes) {
	int sameBytes = 0;
	while (true) {
	    if (sameBytes >= previousBytes.length) {
		return sameBytes;
	    }
	    if (sameBytes >= currentBytes.length) {
		return sameBytes;
	    }
	    if (previousBytes[sameBytes] == currentBytes[sameBytes]) {
		sameBytes++;
	    } else {
		return sameBytes;
	    }
	}
    }

    private byte[] getDiffPart(final int sharedBytesLength, final byte[] full) {
	final byte[] out = new byte[full.length - sharedBytesLength];
	System.arraycopy(full, sharedBytesLength, out, 0, out.length);
	return out;
    }

}
