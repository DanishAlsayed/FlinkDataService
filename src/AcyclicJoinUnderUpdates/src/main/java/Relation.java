package src.main.java;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.sun.istack.internal.Nullable;
import com.sun.istack.internal.logging.Logger;

import static org.apache.commons.lang3.StringUtils.isEmpty;

import java.util.*;

import static java.util.Objects.requireNonNull;

public class Relation {
    private static Logger log = Logger.getLogger(Relation.class);
    private final String name;
    private final Set<String> columnNames;
    private final Map<String, Tuple> tuples;
    private final String primaryKeyName;
    private Map<String, Relation> parents;
    private Map<String, Relation> children;
    private RelationStructure structure;
    private Index index;
//TODO: consider having a Map of foreign keys, key= FK name & value=Relation

    /**
     * Class representing a table. First an empty table should be created, then the family structure should be populated followed by populating tuples. This order of construction is necessary.
     * TODO: look up an appropriate construction design pattern that enforces this order and hide the implementation from users.
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
        parents = new HashMap<>();
        children = new HashMap<>();
        requireNonNull(columnNames);
        if (columnNames.size() < 1) {
            throw new RuntimeException("A relation must have at least 1 column. Provided columnNames' size is: " + columnNames.size());
        }
        this.columnNames = ImmutableSet.copyOf(columnNames);
        //TODO: do we need the tuples map if we have index?
        index = new Index();
    }

    //TODO: ensure that candidate tuple actually belongs to the relation (ensure columns exactly match those of the relation as well as tuple's relation name),
    // Currently any instance of class Tuple can be inserted in any instance of Relation
    public boolean insertTuple(Tuple tuple) {
        requireNonNull(tuple);
        String pk = tuple.getPrimaryKeyValue();
        log.info("Inserting Tuple with PK=" + pk + " in Relation " + name);
        if (tuples.containsKey(tuple.getPrimaryKeyValue())) {
            log.info("Tuple with PK=" + pk + "Already exists.");
            return false;
        }
        tuple.state.setRelationChildCount(children.size());
        index.insertTuple(tuple);
        if (structure == RelationStructure.LEAF) {
            tuple.state.setAlive();
        }
        bottomUpStatusUpdate(tuple, Action.INSERT);
        return (tuples.put(pk, tuple) == null);
    }

    public boolean deleteTuple(final String primaryKeyValue) {
        requireNonNull(primaryKeyValue);
        log.info("Deleting Tuple with PK=" + primaryKeyValue + " in Relation " + name);
        if (!tuples.containsKey(primaryKeyValue)) {
            return false;
        }
        Tuple tuple = tuples.get(primaryKeyValue);
        index.deleteTuple(tuple);
        bottomUpStatusUpdate(tuple, Action.DELETE);
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
        populateFamily(parents, children);
    }

    public Map<String, Relation> getChildren() {
        return children;
    }

    public void setChildren(Map<String, Relation> children) {
        requireNonNull(children);
        this.children = children;
        populateFamily(parents, children);
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
        boolean isRoot;
        if (parents == null) {
            isRoot = true;
            this.parents = null;
        } else {
            isRoot = false;
            this.parents = parents;
        }

        boolean isLeaf;
        if (children == null) {
            isLeaf = true;
            this.children = null;
        } else {
            isLeaf = false;
            this.children = children;
        }
        structure = RelationStructure.getStructure(isRoot, isLeaf);
    }

    /**
     * returns null if tuple doesn't exist
     */
    @Nullable
    private Tuple tupleWithForeignKey(String foreignKey, String value) {
        List<Tuple> result = index.getTuple(foreignKey, value);
        int size = result.size();
        if (size > 1) {
            throw new RuntimeException(size + " tuples found with foreign key. Should be at most 1");
        }

        return size == 0 ? null : result.get(0);
    }

    private void bottomUpStatusUpdate(Tuple tuple, Action action) {
        parents.forEach((parentName, parent) -> {
            //Ideally this check should be redundant as parent must contain the PK of this table as an FK, that's why it is its parent.
            if (!parent.columnNames.contains(primaryKeyName)) {
                throw new RuntimeException("Parent " + parentName + " of " + name + "doesn't have PK: " + primaryKeyName + " as a foreign key.");
            }
            Tuple parentTuple = parent.tupleWithForeignKey(primaryKeyName, tuple.getPrimaryKeyValue());
            if (parentTuple == null) {
                return;
            }
            if (action == Action.INSERT) {
                parentTuple.state.incrementState();
            } else {
                parentTuple.state.decrementState();
            }
        });
    }

    private void setAllTuplesAlive() {
        tuples.forEach((k, v) -> v.state.setAlive());
    }

    //TODO: should it be private?
    private static class Index {
        Map<String, Multimap<String, Tuple>> index;

        Index() {
            index = new HashMap<>();
        }

        void insertTuple(Tuple tuple) {
            tuple.getEntries().forEach((k, v) -> {
                Multimap<String, Tuple> entry = index.computeIfAbsent(k, e -> ArrayListMultimap.create());
                entry.put(v, tuple);
                index.put(k, entry);
            });
        }

        void deleteTuple(Tuple tuple) {
            tuple.getEntries().forEach((k, v) -> {
                index.get(k).remove(v, tuple);
            });
        }

        List<Tuple> getTuple(String key, String value) {
            return (List<Tuple>) index.get(key).get(value);
        }
    }

    private static enum Action {
        INSERT,
        DELETE;
    }
}
