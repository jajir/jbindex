package com.coroptis.index.storage;

import com.coroptis.index.simpleindex.CloseableResource;

public interface FileReader extends CloseableResource {

    /**
     * Read one byte. When byte is not available than return -1.
     *
     * @return read byte as int value from 0 to 255 (inclusive). when value -1 is
     *         returned that end of file was reached.
     */
    int read();

    /**
     * Read all bytes to given field.
     *
     * @param bytes required byte array
     * @return Return number of read bytes. When it's -1 than end of file was
     *         reached.
     */
    int read(byte bytes[]);

    void seek(int position);

}
