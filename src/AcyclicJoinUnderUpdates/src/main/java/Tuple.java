package src.main.java;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isEmpty;

public class Tuple implements Serializable {
    transient TupleState state;
    transient private Map<String, Entry> entries;
    transient private final Set<String> columnNames;
    transient private final String primaryKeyValue;
    transient private final String relationName;
    //TODO: Is there a need to provide number of children and parents of the relation in the constructor? Tuples do not need to know the Relation's family structure.
    // This could lead to discrepancies

    /**
     * A class representing a row in a table(Relation). Note that the table's family structure must be known before creating an object of this class.
     * Based on a hashmap where the key's are the column names and the values are the corresponding values in those columns.
     *
     * @param relationName
     * @param primaryKeyValue
     * @param entries
     */
    public Tuple(final String relationName, final String primaryKeyValue, final Map<String, String> entries) {
        state = new TupleState();
        validateEntries(entries);
        populateEntries(entries);
        this.columnNames = ImmutableSet.copyOf(entries.keySet());
        if (isEmpty(primaryKeyValue)) {
            throw new RuntimeException("Primary key value cannot be empty or null");
        }
        if (isEmpty(relationName)) {
            throw new RuntimeException("Relation name cannot be empty or null");
        }
        this.primaryKeyValue = primaryKeyValue;
        this.relationName = relationName;
    }

    public void updateEntry(final String columnName, final String value) {
        requireNonNull(columnName);
        if (!columnExists(columnName)) {
            throw new RuntimeException("Column must exist in order for its value to be updated. The set of columns is immutable");
        }
        requireNonNull(value);
        entries.put(columnName, new Entry(value));
    }

    public boolean isAlive() {
        return state.isAlive();
    }

    public String getPrimaryKeyValue() {
        return primaryKeyValue;
    }

    public Map<String, Entry> getEntries() {
        return entries;
    }

    //TODO: Below implementation allows empty values as the check is only for null, is that okay?
    private static void validateEntries(final Map<String, String> entries) {
        requireNonNull(entries);
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            requireNonNull(entry.getKey());
            requireNonNull(entry.getValue());
        }
    }

    //TODO: confirm if can columns be added/removed.
    private boolean columnExists(final String columnName) {
        return columnNames.contains(columnName);
    }

    private void populateEntries(final Map<String, String> entries) {
        this.entries = new HashMap<>();
        entries.forEach((columnName, value) -> {
            this.entries.put(columnName, new Entry(value));
        });
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tuple tuple = (Tuple) o;
        return Objects.equals(state, tuple.state) &&
                Objects.equals(entries, tuple.entries) &&
                Objects.equals(columnNames, tuple.columnNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(state, entries, columnNames);
    }

    /*@Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        for (Map.Entry<String, Entry> entry : entries.entrySet()) {
            result.append(entry.getKey())
                    .append("->")
                    .append(entry.getValue().value)
                    .append(" ");
        }
        return result.toString();
    }*/

    //TODO: consider adding value data type to the tuple
    public static class Entry implements Serializable {
        private String value;
        private boolean isAlive;

        public Entry(String value) {
            requireNonNull(value);
            this.value = value;
            this.isAlive = false;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public boolean isAlive() {
            return isAlive;
        }

        public void setAlive(boolean alive) {
            isAlive = alive;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Entry entry = (Entry) o;
            return isAlive == entry.isAlive &&
                    value.equals(entry.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(value, isAlive);
        }
    }

}
