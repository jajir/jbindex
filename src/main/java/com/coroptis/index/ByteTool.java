package com.coroptis.index;
/**
 * Simple operations with byte arrays. 
 * 
 * @author jan
 * 
 */
public class ByteTool {
    
    public int howMuchBytesIsSame(final byte[] array1, final byte[] array2) {
        if(array1==null){
            throw new NullPointerException("First byte array is null.");
        }
        if(array2==null){
            throw new NullPointerException("Second byte array is null.");
        }        
        int sameBytes = 0;
        while (true) {
            if (sameBytes >= array1.length) {
                return sameBytes;
            }
            if (sameBytes >= array2.length) {
                return sameBytes;
            }
            if (array1[sameBytes] == array2[sameBytes]) {
                sameBytes++;
            } else {
                return sameBytes;
            }
        }
    }

    public byte[] getRemainingBytesAfterIndex(final int index, final byte[] full) {
        final byte[] out = new byte[full.length - index];
        System.arraycopy(full, index, out, 0, out.length);
        return out;
    }

}
