package com.coroptis.index.scarceindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

import com.coroptis.index.Pair;
import com.coroptis.index.datatype.TypeDescriptorByte;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.MemDirectory;
import com.coroptis.index.sstfile.SstFileWriter;

public class ScarceIndexTest {

     private final static String FILE_NAME = "pok.dat";
    private final TypeDescriptorString stringTd = new TypeDescriptorString();

    
    @Test
    public void test_one_key() throws Exception {
        final ScarceIndex<String> index=  makeIndex(Pair.of("bbb",1));

        assertEquals(1,index.get("bbb")); 
        assertEquals(1,index.get("aaa")); 
        assertNull(index.get("ccc")); 
    }
    
    @Test
    public void test_empty() throws Exception {
        final ScarceIndex<String> index=  makeIndex();

        assertNull(index.get("aaa")); 
        assertNull(index.get("bbb")); 
        assertNull(index.get("ccc")); 
    }
    
    @Test
    public void test_one_multiple() throws Exception {
        final ScarceIndex<String> index=  makeIndex(Pair.of("bbb",1),
        Pair.of("ccc",2),
        Pair.of("ddd",3),
        Pair.of("eee",4),
        Pair.of("fff",5));

        assertEquals(1,index.get("bbb")); 
        assertEquals(2,index.get("ccc")); 
        assertEquals(3,index.get("ccd")); 
        assertEquals(3,index.get("cee")); 
        assertNull(index.get("ggg")); 
    }

    
    @Test
    public void test_overwrite_index() throws Exception {
        final ScarceIndex<String> index=  makeIndex(Pair.of("bbb",1),
        Pair.of("ccc",2),
        Pair.of("ddd",3),
        Pair.of("eee",4),
        Pair.of("fff",5));

        try(final ScarceIndexWriter<String> writer= index.openWriter()) {
            writer.put(Pair.of("bbb",1));
        }

        assertEquals(1,index.get("bbb")); 
        assertEquals(1,index.get("aaa")); 
        assertNull(index.get("ccc")); 
    }

    private ScarceIndex<String> makeIndex(Pair<String,Integer>... pairs){
        final MemDirectory directory = new MemDirectory();
        final ScarceIndex<String> index= ScarceIndex.<String>builder().withDirectory(directory).withFileName(FILE_NAME).withKeyTypeDescriptor(stringTd).build();

        try(final ScarceIndexWriter<String> writer= index.openWriter()) {
            for(int i=0;i<pairs.length;i++){
                final Pair<String,Integer> one = pairs[i];
                writer.put(one);
            }
        }

        return index;
    }
}
