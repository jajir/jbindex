package com.coroptis.index.storage;

import java.io.File;
import java.util.Objects;

import com.coroptis.index.simpleindex.IndexException;

public class FsDirectory implements Directory {

    private final File directory;

    public FsDirectory(final File directory) {
	this.directory = Objects.requireNonNull(directory);
	if (!directory.exists()) {
	    directory.mkdirs();
	}
	if (directory.isFile()) {
	    throw new IndexException(String.format("There is required directory but '%s' is file."));
	}
    }

    @Override
    public FileReader getFileReader(final String fileName) {
	final File file = getFile(fileName);
	if (!directory.exists()) {
	    throw new IndexException(String.format("File '%s' doesn't exists."));
	}
	return new FsFileReader(file);
    }

    @Override
    public FileWriter getFileWriter(final String fileName) {
	Objects.requireNonNull(fileName);
	return new FsFileWriter(getFile(fileName));
    }

    private File getFile(final String fileName) {
	Objects.requireNonNull(fileName);
	return directory.toPath().resolve(fileName).toFile();
    }

}
