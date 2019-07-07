package com.coroptis.index.type;

@Deprecated
public class TypeDescriptorInteger {
    // FIXME remove this class

    /**
     * How many bytes is required to store Integer.
     */
    private static final int REQUIRED_BYTES = 4;

    /**
     * With byte AND allows to select required part of bytes.
     */
    private static final int BYTE_MASK = 0xFF;

    /**
     * Bite shift for 0 bits.
     */
    private static final int BYTE_SHIFT_0 = 0;

    /**
     * Bite shift for 8 bits.
     */
    private static final int BYTE_SHIFT_8 = 8;

    /**
     * Bite shift for 16 bits.
     */
    private static final int BYTE_SHIFT_16 = 16;

    /**
     * Bite shift for 24 bits.
     */
    private static final int BYTE_SHIFT_24 = 24;

    /**
     * Default hash code.
     */
    private static final int DEFAULT_HASHCODE = 7312485;

    public Integer load(final byte[] data, final int from) {
	int pos = from;
	return data[pos++] << BYTE_SHIFT_24 | (data[pos++] & BYTE_MASK) << BYTE_SHIFT_16
		| (data[pos++] & BYTE_MASK) << BYTE_SHIFT_8 | (data[pos] & BYTE_MASK);
    }

    public String toString() {
	return "TypeDescriptorInteger{maxLength=4}";
    }

    /**
     * Always return same number. All instances of this class are same.
     */
    public int hashCode() {
	return DEFAULT_HASHCODE;
    }

    public boolean equals(final Object obj) {
	if (this == obj) {
	    return true;
	}
	if (obj == null) {
	    return false;
	}
	return getClass() == obj.getClass();
    }

    public byte[] getBytes(final Integer value) {
	int pos = 0;
	int v = value.intValue();
	byte[] out = new byte[REQUIRED_BYTES];
	out[pos++] = (byte) ((v >>> BYTE_SHIFT_24) & BYTE_MASK);
	out[pos++] = (byte) ((v >>> BYTE_SHIFT_16) & BYTE_MASK);
	out[pos++] = (byte) ((v >>> BYTE_SHIFT_8) & BYTE_MASK);
	out[pos] = (byte) ((v >>> BYTE_SHIFT_0) & BYTE_MASK);
	return out;
    }

}
