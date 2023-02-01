package com.coroptis.index.simpledatafile;

public class Stats {

    private final long numberOfPairsInCache;
    private final long numberOfPairsInMainFile;

    Stats(final long numberOfPairsInCache, final long numberOfPairsInMainFile) {
	this.numberOfPairsInCache = numberOfPairsInCache;
	this.numberOfPairsInMainFile = numberOfPairsInMainFile;
    }

    public long getNumberOfPairsInCache() {
	return numberOfPairsInCache;
    }

    public long getNumberOfPairsInMainFile() {
	return numberOfPairsInMainFile;
    }

    public long getTotalNumberOfPairs() {
	return getNumberOfPairsInCache() + getNumberOfPairsInMainFile();
    }
}
