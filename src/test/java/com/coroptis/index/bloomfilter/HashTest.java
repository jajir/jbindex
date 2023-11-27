package com.coroptis.index.bloomfilter;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

public class HashTest {
    @Test
    void testName() throws Exception {
        Hash hash = new Hash();
        
        System.out.println(hash.putBinary("ahoj".getBytes()));
    }

}
