package src.main.java;

import java.util.Objects;

public class State {
    private boolean isAlive = false;
    private int stateCount = 0;
    private final int relationChildCount;

    public State(final int relationChildCount) {
        this.relationChildCount = relationChildCount;
    }

    public boolean isAlive() {
        return isAlive;
    }

    public void setAlive(boolean alive) {
        isAlive = alive;
    }

    public int getRelationChildCount() {
        return relationChildCount;
    }

    public void incrementState() {
        this.stateCount++;
        if (this.stateCount == relationChildCount) {
            this.isAlive = true;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        State state = (State) o;
        return isAlive == state.isAlive &&
                stateCount == state.stateCount &&
                relationChildCount == state.relationChildCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(isAlive, stateCount, relationChildCount);
    }
}
