package com.coroptis.index.directory;

import java.util.stream.Stream;

public interface Directory {

    FileReader getFileReader(String fileName);

    FileWriter getFileWriter(String fileName);

    boolean deleteFile(String fileName);

    Stream<String> getFileNames();

}
