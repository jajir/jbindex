package com.coroptis.index.directory;

import java.util.Objects;
import java.util.stream.Stream;

public interface Directory {

    public static enum Access {
        APPEND, OVERWRITE
    }

    FileReader getFileReader(String fileName);

    /**
     * Opens writer to file. When file already exists than method override it.
     * 
     * @param fileName required file name in this directory
     * @return return {@link FileWriter} or exception is thrown
     */
    default FileWriter getFileWriter(final String fileName) {
        return getFileWriter(
                Objects.requireNonNull(fileName,
                        () -> String.format("File name is null.")),
                Access.OVERWRITE);
    }

    boolean isFileExists(final String fileName);

    FileWriter getFileWriter(String fileName, Access access);

    boolean deleteFile(String fileName);

    Stream<String> getFileNames();

    void renameFile(String currentFileName, String newFileName);

}
