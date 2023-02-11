package com.coroptis.index.sorteddatafile;

import com.coroptis.index.IndexException;
import com.coroptis.index.directory.FileReader;
import com.coroptis.index.type.ConvertorFromBytes;
import com.coroptis.index.type.TypeReader;

public class DiffKeyReader<K> implements TypeReader<K> {

    private final ConvertorFromBytes<K> keyConvertor;

    private byte[] previousKeyBytes;

    public DiffKeyReader(final ConvertorFromBytes<K> keyConvertor) {
        this.keyConvertor = keyConvertor;
        previousKeyBytes = null;
    }

    @Override
    public K read(final FileReader fileReader) {
        final int sharedByteLength = fileReader.read();
        if (sharedByteLength == -1) {
            return null;
        }
        final int keyLengthInBytes = fileReader.read();
        if (sharedByteLength == 0) {
            final byte[] keyBytes = new byte[keyLengthInBytes];
            fileReader.read(keyBytes);
            previousKeyBytes = keyBytes;
            return keyConvertor.fromBytes(keyBytes);
        }
        if (previousKeyBytes == null) {
            throw new IndexException(String
                    .format("Unable to read key because there should be '%s' "
                            + "bytes shared with previous key but there is no"
                            + " previous key", sharedByteLength));
        }
        if (previousKeyBytes.length < sharedByteLength) {
            final String s1 = new String(previousKeyBytes);
            throw new IndexException(String.format(
                    "Previous key is '%s' with length '%s'. "
                            + "Current key should share '%s' with previous key.",
                    s1, previousKeyBytes.length, sharedByteLength));
        }
        final byte[] diffBytes = new byte[keyLengthInBytes];
        fileReader.read(diffBytes);
        final byte[] sharedBytes = getBytes(previousKeyBytes, sharedByteLength);
        final byte[] keyBytes = concatenateArrays(sharedBytes, diffBytes);
        previousKeyBytes = keyBytes;
        return keyConvertor.fromBytes(keyBytes);
    }

    private byte[] getBytes(final byte[] bytes, final int howMany) {
        final byte[] out = new byte[howMany];
        System.arraycopy(bytes, 0, out, 0, howMany);
        return out;
    }

    private byte[] concatenateArrays(final byte[] firstBytes,
            final byte[] secondBytes) {
        final byte[] out = new byte[firstBytes.length + secondBytes.length];
        System.arraycopy(firstBytes, 0, out, 0, firstBytes.length);
        System.arraycopy(secondBytes, 0, out, firstBytes.length,
                secondBytes.length);
        return out;
    }

}
