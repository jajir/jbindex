package com.coroptis.index.type;

import com.coroptis.index.directory.FileWriter;

public interface TypeWriter<T> {

    int write(FileWriter fileWriter, T object);

}
