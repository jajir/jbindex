package com.coroptis.index.bloomfilter;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class BitArray2 {
    
    private final byte[] data;
    private long bitCount;
    
    public boolean get(final int index) {
        
    }
    
    public boolean set(final int index) {
        
    }

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;
        if (!(other instanceof BitArray2))
            return false;
        BitArray2 that = (BitArray2) other;
        return Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }
}
