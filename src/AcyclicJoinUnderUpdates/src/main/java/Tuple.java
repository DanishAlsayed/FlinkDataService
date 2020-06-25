package src.main.java;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isEmpty;

public class Tuple {
    private TupleState state;
    private Map<String, String> entries;
    private final Set<String> columnNames;
    private final String primaryKeyValue;
    private final String relationName;
    private final int relationParentCount;
    //TODO: Is there a need to provide number of children and parents of the relation in the constructor? Tuples do not need to know the Relation's family structure.
    // This could lead to discrepancies
    /**
     * A class representing a row in a table(Relation). Note that the table's family structure must be known before creating an object of this class.
     * Based on a hashmap where the key's are the column names and the values are the corresponding values in those columns.
     * @param relationName
     * @param relationChildCount
     * @param relationParentCount
     * @param primaryKeyValue
     * @param entries
     */
    public Tuple(final String relationName, final int relationChildCount, final int relationParentCount, final String primaryKeyValue, final Map<String, String> entries) {
        state = new TupleState(relationChildCount);
        validateEntries(entries);
        this.entries = entries;
        this.columnNames = ImmutableSet.copyOf(entries.keySet());
        if (isEmpty(primaryKeyValue)) {
            throw new RuntimeException("Primary key value cannot be empty or null");
        }
        if (isEmpty(relationName)) {
            throw new RuntimeException("Relation name cannot be empty or null");
        }

        if (relationParentCount < 0) {
            throw new RuntimeException("relationParentCount cannot be less than 0");
        }
        this.primaryKeyValue = primaryKeyValue;
        this.relationName = relationName;
        this.relationParentCount = relationParentCount;
    }

    public void updateEntry(final String columnName, final String value) {
        requireNonNull(columnName);
        if (!columnExists(columnName)) {
            throw new RuntimeException("Column must exist in order for its value to be updated. The set of columns is immutable");
        }
        requireNonNull(value);
        entries.put(columnName, value);
    }

    public boolean isAlive() {
        return state.isAlive();
    }

    public String getPrimaryKeyValue() {
        return primaryKeyValue;
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
}
