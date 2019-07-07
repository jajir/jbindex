package com.coroptis.index.type;

import com.coroptis.index.storage.FileReader;

public interface TypeStreamReader<T> {

    T read(FileReader inputStream);

}
