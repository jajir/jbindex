package com.coroptis.index.rigidindex;

import java.util.Objects;

import com.coroptis.index.IndexException;
import com.coroptis.index.basic.ValueMerger;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.type.TypeDescriptor;

public class SimpleIndex<K, V> {

    public static enum State {

        SEARCHING("Searching"), WRITING("Writing"), CLOSED("Closed");

        private final String name;

        State(final String name) {
            this.name = Objects.requireNonNull(name);
        }

        public String getName() {
            return name;
        }

    }

    private final Directory directory;
    private final ValueMerger<K, V> valueMerger;
    private TypeDescriptor<K> keyTypeDescriptor;
    private TypeDescriptor<V> valueTypeDescriptor;

    private State actualState;

    public static <M, N> SimpleIndexBuilder<M, N> builder() {
        return new SimpleIndexBuilder<M, N>();
    }

    public SimpleIndex(final Directory directory, final ValueMerger<K, V> valueMerger,
            TypeDescriptor<K> keyTypeDescriptor, TypeDescriptor<V> valueTypeDescriptor) {
        this.directory = Objects.requireNonNull(directory);
        this.valueMerger = Objects.requireNonNull(valueMerger);
        this.keyTypeDescriptor = Objects.requireNonNull(keyTypeDescriptor);
        this.valueTypeDescriptor = Objects.requireNonNull(valueTypeDescriptor);
    }

    public void startWriting() {
        checkRequiredState(State.SEARCHING);
        actualState = State.WRITING;
        actualState = State.SEARCHING;
    }

    public void close() {
        checkRequiredState(State.SEARCHING);
    }

    private void checkRequiredState(final State requiredState) {
        if (!actualState.equals(requiredState)) {
            throw new IndexException(
                    String.format("Index should be in state '%s' but it's in state '%s'",
                            requiredState, actualState));
        }
    }

}
