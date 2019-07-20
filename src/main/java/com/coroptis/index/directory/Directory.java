package com.coroptis.index.directory;

public interface Directory {

    FileReader getFileReader(String fileName);

    FileWriter getFileWriter(String fileName);

}
