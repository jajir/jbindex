package com.coroptis.index.bloomfilter;

import org.apache.commons.codec.digest.MurmurHash3;

/**
 * Implementation was taken from <a href=
 * "https://github.com/apache/spark/blob/93251ed77ea1c5d037c64d2292b8760b03c8e181/common/sketch/src/main/java/org/apache/spark/util/sketch/BloomFilterImpl.java">
 * https://github.com/apache/spark/blob/93251ed77ea1c5d037c64d2292b8760b03c8e181/common/sketch/src/main/java/org/apache/spark/util/sketch/BloomFilterImpl.java
 * </a>
 * 
 * @author honza
 *
 */
public class Hash {

    private BitArray bits;

    private int numHashFunctions;

    public boolean putBinary(final byte[] item) {
        int h1 = MurmurHash3.hash32x86(item, 0, item.length, 0);
        int h2 = MurmurHash3.hash32x86(item, 0, item.length, h1);

        long bitSize = bits.bitSize();
        boolean bitsChanged = false;
        for (int i = 1; i <= numHashFunctions; i++) {
            int combinedHash = h1 + (i * h2);
            // Flip all the bits if it's negative (guaranteed positive number)
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            bitsChanged |= bits.set(combinedHash % bitSize);
        }
        return bitsChanged;
    }
}
