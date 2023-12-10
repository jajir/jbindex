package com.coroptis.index.bloomfilter;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class HashTest {
    @Test
    void testStore_simple() throws Exception {
        Hash hash = new Hash(new BitArray(10), 3);

        assertTrue(hash.store("ahoj".getBytes()));
        assertFalse(hash.store("ahoj".getBytes()));
        assertFalse(hash.store("ahoj".getBytes()));
    }

    @Test
    void testStore_null_data() throws Exception {
        Hash hash = new Hash(new BitArray(10), 3);

        assertThrows(NullPointerException.class, () -> hash.store(null));
    }

    @Test
    void testStore_zero_data() throws Exception {
        Hash hash = new Hash(new BitArray(10), 3);

        assertThrows(IllegalArgumentException.class,
                () -> hash.store(new byte[0]));
    }

    @Test
    void testIsNotStored_null_data() throws Exception {
        Hash hash = new Hash(new BitArray(10), 3);

        assertThrows(NullPointerException.class, () -> hash.isNotStored(null));
    }

    @Test
    void testIsNotStored_zero_data() throws Exception {
        Hash hash = new Hash(new BitArray(10), 3);

        assertThrows(IllegalArgumentException.class,
                () -> hash.isNotStored(new byte[0]));
    }

    @Test
    void testIsNotStored_simple() throws Exception {
        Hash hash = new Hash(new BitArray(10), 10);

        assertTrue(hash.isNotStored("ahoj".getBytes()));
        hash.store("ahoj".getBytes());
        assertFalse(hash.isNotStored("ahoj".getBytes()));
        assertTrue(hash.isNotStored("kachna".getBytes()));
    }

    @Test
    void testConstructor_InvalidNumberOfHashFunctions() throws Exception {
        assertThrows(IllegalArgumentException.class,
                () -> new Hash(new BitArray(10), 0));
    }

}
