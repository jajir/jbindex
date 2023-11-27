package com.coroptis.index.bloomfilter;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class BitArray {
    
    private final byte[] byteArray;
    
    public BitArray(int length) {
        byteArray = new byte[length];
    }
    
    public boolean setBit(int index) {
        if (index < 0 || index >= byteArray.length * 8) {
            throw new IndexOutOfBoundsException("Invalid index");
        }

        int byteIndex = index / 8;
        int bitIndex = index % 8;

        byte oldValue = byteArray[byteIndex];
        byte newValue = (byte) (oldValue | (1 << bitIndex));
        byteArray[byteIndex] = newValue;

        return oldValue != newValue;
    }

    public byte[] getByteArray() {
        return byteArray;
    }

    public boolean get(final int index) {
        return false;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;
        if (!(other instanceof BitArray))
            return false;
        BitArray that = (BitArray) other;
        return Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }
}
