package com.coroptis.index.storage;

public interface Directory {

    FileReader getFileReader(String fileName);

    FileWriter getFileWriter(String fileName);

}
