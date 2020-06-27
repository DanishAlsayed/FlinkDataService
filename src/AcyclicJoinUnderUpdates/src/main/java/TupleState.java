package src.main.java;

import java.util.Objects;

public class TupleState {
    private boolean isAlive = false;
    private int stateCount = 0;
    private int relationChildCount = -1;

    public TupleState() {
    }

    public boolean isAlive() {
        return isAlive;
    }

    public void setRelationChildCount(int count) {
        if (count < 0) {
            throw new RuntimeException("relationChildCount cannot be less than 0");
        }
        if (relationChildCount >= 0) {
            throw new RuntimeException("relationChildCount cannot be set more than once. relationChildCount=" + relationChildCount);
        }
        relationChildCount = count;
    }

    public int getRelationChildCount() {
        return relationChildCount;
    }

    public void incrementState() {
        checkRelationChildCount();
        if (stateCount == relationChildCount) {
            throw new RuntimeException("StateCount=relationChildCount=" + stateCount + ". Cannot increment any further.");
        }
        stateCount++;
        if (stateCount == relationChildCount) {
            isAlive = true;
        }
    }

    public void decrementState() {
        checkRelationChildCount();
        if (stateCount <= 0) {
            throw new RuntimeException("State count is: " + stateCount + ". Cannot be decremented any further.");
        }
        stateCount--;
        isAlive = false;
    }

    /**
     * only to be used when containing relation is leaf
     */
    public void setAlive() {
        isAlive = true;
    }

    private void checkRelationChildCount() {
        if (relationChildCount < 0) {
            throw new RuntimeException("Must set relationChildCount before using state");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TupleState state = (TupleState) o;
        return isAlive == state.isAlive &&
                stateCount == state.stateCount &&
                relationChildCount == state.relationChildCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(isAlive, stateCount, relationChildCount);
    }
}
