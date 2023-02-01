package com.coroptis.index.fastindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;
import com.coroptis.index.type.TypeDescriptorString;

public class FastIndexFileTest {

    private final TypeDescriptorString stringTd = new TypeDescriptorString();

    @Test
    public void test_insert_firts_page() throws Exception {
	final Directory directory = new MemDirectory();
	try (final FastIndexFile<String> fif = new FastIndexFile<>(directory, stringTd)) {

	    assertNull(fif.findFileId("test"));

	    fif.insertPage("ahoj", 1);
	    fif.insertPage("betka", 2);
	    fif.insertPage("cukrar", 3);
	    fif.insertPage("dikobraz", 4);

	    assertEquals(3, fif.findFileId("cuketa"));
	    assertEquals(3, fif.findFileId("bziknout"));
	    assertEquals(4, fif.insertKeyToPage("kachna"));
	}

	try (final FastIndexFile<String> fif = new FastIndexFile<>(directory, stringTd)) {
	    assertEquals(3, fif.findFileId("cuketa"));
	    assertEquals(3, fif.findFileId("bziknout"));
	    assertEquals(4, fif.findFileId("kachna"));
	}
    }

}
