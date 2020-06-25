package src.main.java;

import com.google.common.collect.ImmutableSet;

import static org.apache.commons.lang3.StringUtils.isEmpty;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class Relation {
    private final String name;
    private final Set<String> columnNames;
    private final Map<String, Tuple> tuples;
    private final String primaryKeyName;
    private Map<String, Relation> parents;
    private Map<String, Relation> children;
    private boolean isRoot;
    private boolean isLeaf;
    private RelationStructure structure;

    /**
     * Class representing a table. First an empty table should be created, then the tuples and family structure should be populated
     *
     * @param name
     * @param primaryKeyName
     */
    public Relation(final String name, final String primaryKeyName, final Set<String> columnNames) {
        if (isEmpty(primaryKeyName) || isEmpty(name)) {
            throw new RuntimeException("Primary key name & relation name cannot be empty or null." +
                    " name = " + (name == null ? "null" : name) + ", primaryKeyName = " + (primaryKeyName == null ? "null" : primaryKeyName));
        }

        this.primaryKeyName = primaryKeyName;
        this.name = name;
        tuples = new HashMap<>();
        this.parents = new HashMap<>();
        this.children = new HashMap<>();
        requireNonNull(columnNames);
        if (columnNames.size() < 1) {
            throw new RuntimeException("A relation must have at least 1 column. Provided columnNames' size is: " + columnNames.size());
        }
        this.columnNames = ImmutableSet.copyOf(columnNames);
    }

    //TODO: ensure that candidate tuple actually belongs to the relation (ensure columns exactly match those of the relation as well as tuple's relation name),
    // Currently any instance of class Tuple can be inserted in any instance of Relation
    public void insertTuple(final Tuple tuple) {
        requireNonNull(tuple);
        this.tuples.put(tuple.getPrimaryKeyValue(), tuple);
    }

    public boolean deleteTuple(final String primaryKeyValue) {
        requireNonNull(primaryKeyValue);
        if (!tuples.containsKey(primaryKeyValue)) {
            return false;
        }
        return (tuples.remove(primaryKeyValue) != null);
    }

    public int numberOfChildren() {
        return children.size();
    }

    public int numberOfParents() {
        return parents.size();
    }

    public Map<String, Relation> getParents() {
        return parents;
    }

    public void setParents(Map<String, Relation> parents) {
        requireNonNull(parents);
        this.parents = parents;
        populateFamily(parents, this.children);
    }

    public Map<String, Relation> getChildren() {
        return children;
    }

    public void setChildren(Map<String, Relation> children) {
        requireNonNull(children);
        this.children = children;
        populateFamily(this.parents, children);
    }

    public RelationStructure getStructure() {
        return structure;
    }

    public Set<String> getColumnNames() {
        return columnNames;
    }

    public String getName() {
        return name;
    }

    public Map<String, Tuple> getTuples() {
        return tuples;
    }

    /**
     * @param parents  null if root
     * @param children null if leaf
     */
    public void populateFamily(final Map<String, Relation> parents, final Map<String, Relation> children) {
        if (parents == null) {
            this.isRoot = true;
            this.parents = null;
        } else {
            this.isRoot = false;
            this.parents = parents;
        }

        if (children == null) {
            this.isLeaf = true;
            this.children = null;
        } else {
            this.isLeaf = false;
            this.children = children;
        }
        this.structure = RelationStructure.getStructure(isRoot, isLeaf);
    }
}
