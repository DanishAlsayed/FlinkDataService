package src.main.java;

//import com.sun.istack.internal.logging.Logger;

import java.io.Serializable;
import java.util.Objects;

public class TupleState implements Serializable {
    //private static Logger log = Logger.getLogger(TupleState.class);
    private boolean isAlive = false;
    private int stateCount = 0;
    private int relationChildCount = -1;

    public TupleState() {
    }

    public boolean isAlive() {
        return isAlive;
    }

    //TODO: This method must be called for a TupleState instance to be functional
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

    public void incrementState(String relationName, String tuplePKValue) {
        checkRelationChildCount();
        if (stateCount == relationChildCount) {
            //log.info("StateCount=relationChildCount=" + stateCount + ". Cannot increment any further. For " + relationName + " PK=" + tuplePKValue);
            return;
        }
        stateCount++;
        //log.info("Tuple with PK=" + tuplePKValue + " in relation " + relationName + "'s stateCount is updated, stateCount=" + stateCount);
        if (stateCount == relationChildCount) {
            isAlive = true;
            //log.info("Tuple with PK=" + tuplePKValue + " in relation " + relationName + " is now alive. relationChildCount=" + relationChildCount);
        }
    }

    public void decrementState(String relationName, String tuplePKValue) {
        checkRelationChildCount();
        if (stateCount <= 0) {
            throw new RuntimeException("State count is: " + stateCount + ". Cannot be decremented any further.");
        }
        stateCount--;
        isAlive = false;
        //log.info("Tuple with PK=" + tuplePKValue + " in relation " + relationName + " is now NOT alive. relationChildCount=" + relationChildCount + ", stateCount=" + stateCount);
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
