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
    private final Set<String> columns;
    private final String primaryKeyValue;

    public Tuple(final int relationChildCount, final Map<String, String> entries, final String primaryKeyValue) {
        state = new TupleState(relationChildCount);
        validateEntries(entries);
        this.entries = entries;
        this.columns = ImmutableSet.copyOf(entries.keySet());
        if (isEmpty(primaryKeyValue)) {
            throw new RuntimeException("Primary key value cannot be empty or null");
        }
        this.primaryKeyValue = primaryKeyValue;
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

    private static void validateEntries(final Map<String, String> entries) {
        requireNonNull(entries);
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            requireNonNull(entry.getKey());
            requireNonNull(entry.getValue());
        }
    }

    //TODO: confirm if can columns be added/removed.
    private boolean columnExists(final String columnName) {
        return columns.contains(columnName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tuple tuple = (Tuple) o;
        return Objects.equals(state, tuple.state) &&
                Objects.equals(entries, tuple.entries) &&
                Objects.equals(columns, tuple.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(state, entries, columns);
    }
}
