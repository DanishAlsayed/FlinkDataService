package src.main.java;

import static org.apache.commons.lang3.StringUtils.isEmpty;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class Relation {
    private final String name;
    private final Map<String, Tuple> rows;
    private final String primaryKeyName;
    private Map<String, Relation> parents;
    private Map<String, Relation> children;
    private boolean isRoot;
    private boolean isLeaf;
    private RelationStructure structure;

    public Relation(final String name, final String primaryKeyName, final Tuple tuple,
                    final Map<String, Relation> parents, Map<String, Relation> children) {
        if (isEmpty(primaryKeyName) || isEmpty(name)) {
            throw new RuntimeException("Primary key name & relation name cannot be empty or null." +
                    " name = " + (name == null ? "null" : name) + ", primaryKeyName = " + (primaryKeyName == null ? "null" : primaryKeyName));
        }
        requireNonNull(tuple);
        structure = populateFamily(parents, children);
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

    public Map<String, Relation> getParents() {
        return parents;
    }

    public void setParents(Map<String, Relation> parents) {
        requireNonNull(parents);
        this.parents = parents;
        structure = populateFamily(parents, this.children);
    }

    public Map<String, Relation> getChildren() {
        return children;
    }

    public void setChildren(Map<String, Relation> children) {
        requireNonNull(children);
        this.children = children;
        structure = populateFamily(this.parents, children);
    }

    public RelationStructure getStructure() {
        return structure;
    }

    private RelationStructure populateFamily(final Map<String, Relation> parents, final Map<String, Relation> children) {
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

        return RelationStructure.getStructure(isRoot, isLeaf);
    }
}
