package com.coroptis.index.jbindex;

import java.util.Objects;

public class JbIndex<K, V> {

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

    private State actualState;

    public void startWriting() {
	checkRequiredState(State.SEARCHING);
	actualState = State.WRITING;
	actualState = State.SEARCHING;
    }

    public void close() {
	checkRequiredState(State.SEARCHING);
	// FIXME do close
    }

    private void checkRequiredState(final State requiredState) {
	if (!actualState.equals(requiredState)) {
	    throw new IndexException(
		    String.format("Index should be in state '%s' but it's in state '%s'", requiredState, actualState));
	}
    }

}
