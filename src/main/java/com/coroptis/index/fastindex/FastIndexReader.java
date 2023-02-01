package com.coroptis.index.fastindex;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.coroptis.index.IndexException;
import com.coroptis.index.Pair;
import com.coroptis.index.PairFileReader;
import com.coroptis.index.simpledatafile.SimpleDataFile;

public class FastIndexReader<K, V> implements PairFileReader<K, V> {

    private final List<Integer> pageIdsd;
    private final FastIndex<K, V> fastIndex;
    private SimpleDataFile<K, V> currentSegment;
    private PairFileReader<K, V> currentReader;

    FastIndexReader(final FastIndex<K, V> fastIndex, final FastIndexFile<K> fastIndexFile) {
	this.fastIndex = Objects.requireNonNull(fastIndex);
	this.pageIdsd = fastIndexFile.getPagesAsStream().map(pair -> pair.getValue()).collect(Collectors.toList());
	loadNextSegment();
    }

    @Override
    public Pair<K, V> read() {
	if (currentReader == null) {
	    return null;
	}
	final Pair<K, V> out = currentReader.read();
	if (out == null) {
	    loadNextSegment();
	    return read();
	}
	return out;
    }

    private void loadNextSegment() {
	if (currentReader != null) {
	    currentReader.close();
	    currentReader = null;
	}
	if (pageIdsd.size() > 0) {
	    final Integer segmentId = pageIdsd.remove(0);
	    currentSegment = fastIndex.getSegment(segmentId);
	    currentReader = currentSegment.openReader();
	}
    }

    @Override
    public void skip(long position) {
	throw new IndexException("not implemented");
    }

    @Override
    public void close() {
	// TODO Auto-generated method stub

    }

}
