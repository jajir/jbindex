package com.coroptis.index.storage;

import com.coroptis.index.simpleindex.CloseableResource;

public interface FileWriter extends CloseableResource {

    void write(byte b);

    void write(byte bytes[]);

}
