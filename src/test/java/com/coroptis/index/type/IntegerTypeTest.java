package com.coroptis.index.type;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.util.Comparator;

import org.junit.jupiter.api.Test;

import com.coroptis.index.DataFileReader;
import com.coroptis.index.basic.BasicIndex;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.FsDirectory;
import com.coroptis.index.directory.MemDirectory;
import com.coroptis.index.type.TypeDescriptorByte;
import com.coroptis.index.type.TypeDescriptorString;


public class IntegerTypeTest {

    private final TypeDescriptorInteger ti = new TypeDescriptorInteger();
    private final ConvertorToBytes<Integer> toBytes = ti.getConvertorToBytes();
    private final ConvertorFromBytes<Integer> fromBytes = ti.getConvertorFromBytes();

    @Test
    public void test_convertorto_bytes() throws Exception {


        assertEqualsBytes(0);
        assertEqualsBytes(Integer.MAX_VALUE);
        assertEqualsBytes(Integer.MIN_VALUE);
        assertEqualsBytes(-1);
    }  

    

    private void assertEqualsBytes(Integer number){
        final byte[] bytes = toBytes.toBytes(number);
        final Integer ret = fromBytes.fromBytes(bytes);
        assertEquals(number,ret,String.format("Expected '%s' byt returned was '%s'", number,ret));
    }
}
