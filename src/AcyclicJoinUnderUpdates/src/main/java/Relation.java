package src.main.java;

import static org.apache.commons.lang3.StringUtils.isEmpty;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class Relation {
    private final String name;
    private final Map<String, Tuple> rows;
    private final String primaryKeyName;

    public Relation(final String name, final String primaryKeyName, final Tuple tuple) {
        if (isEmpty(primaryKeyName) || isEmpty(name)) {
            throw new RuntimeException("Primary key name & relation name cannot be empty or null." +
                    " name = " + (name == null ? "null" : name) + ", primaryKeyName = " + (primaryKeyName == null ? "null" : primaryKeyName));
        }
        requireNonNull(tuple);
        this.primaryKeyName = primaryKeyName;
        this.name = name;
        rows = new HashMap<>();
        rows.put(tuple.getPrimaryKeyValue(), tuple);
    }

    public void insertTuple(final Tuple tuple) {
        requireNonNull(tuple);
        this.rows.put(tuple.getPrimaryKeyValue(), tuple);
    }

    public boolean deleteTuple(final String primaryKeyValue) {
        requireNonNull(primaryKeyValue);
        if (!rows.containsKey(primaryKeyValue)) {
            return false;
        }
        return (rows.remove(primaryKeyValue) != null);
    }
}
