package com.coroptis.index.type;

import com.coroptis.index.directory.FileReader;

public interface TypeReader<T> {

    T read(FileReader reader);

}
