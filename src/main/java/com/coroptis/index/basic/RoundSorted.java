package com.coroptis.index.basic;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import com.coroptis.index.Pair;

public class RoundSorted<K, V> {

    private final SortSupport<K, V> sortSupport;
    private final BasicIndex<K, V> basicIndex;
    private final int howManyFilesMergeAtOnce;

    RoundSorted(final BasicIndex<K, V> basicIndex, final SortSupport<K, V> sortSupport,
            final int howManyFilesMergeAtOnce) {
        this.sortSupport = Objects.requireNonNull(sortSupport);
        this.basicIndex = Objects.requireNonNull(basicIndex);
        this.howManyFilesMergeAtOnce = howManyFilesMergeAtOnce;
    }

    boolean mergeRound(final int roundNo, final Consumer<Pair<K, V>> consumer) {
        final List<String> filesToMerge = sortSupport.getFilesInRound(roundNo);

        if (filesToMerge.size() == 0) {
            // do nothing
            return false;
        }

        final int howManyFilesShouldBeProduces = howManyFilesShouldBeProduces(filesToMerge.size());
        final boolean isFinalMergingRound = howManyFilesShouldBeProduces == 1;

        for (int i = 0; i < howManyFilesShouldBeProduces; i++) {
            final List<String> filesToMergeLocaly = new ArrayList<>();
            for (int j = 0; j < howManyFilesMergeAtOnce; j++) {
                final int index = i * howManyFilesMergeAtOnce + j;
                if (index < filesToMerge.size()) {
                    final String fileName = filesToMerge.get(index);
                    filesToMergeLocaly.add(fileName);
                }
            }
            if (isFinalMergingRound) {
                /*
                 * When it's final merging round data should be send to caller.
                 */
                sortSupport.mergeSortedFiles(filesToMergeLocaly, consumer::accept);
            } else {
                sortSupport.mergeSortedFiles(filesToMergeLocaly,
                        sortSupport.makeFileName(roundNo + 1, i));
            }
            filesToMergeLocaly.forEach(fileName -> basicIndex.deleteFile(fileName));
        }

        return !isFinalMergingRound;
    }

    /**
     * Divide number of files which should be merged by how many files should be
     * merged at once. Result is rounded up.
     * 
     * @param numberOfFiles return number of round which should merge files
     * @return
     */
    private int howManyFilesShouldBeProduces(final int numberOfFiles) {
        return (int) Math.ceil(((float) numberOfFiles) / ((float) howManyFilesMergeAtOnce));
    }

}
