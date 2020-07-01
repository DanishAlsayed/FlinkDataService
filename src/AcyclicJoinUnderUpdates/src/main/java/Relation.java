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
     * DAG condition is PK->FK, opposite to the paper, same as the presentation.
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

    public boolean insertTuple(Tuple tuple) {
        requireNonNull(tuple);
        Set<String> tupleColumnNames = tuple.getEntries().keySet();
        if (tupleColumnNames.size() != columnNames.size() || !tupleColumnNames.containsAll(columnNames)) {
            throw new RuntimeException("Column names for tuple and relation do not match. Tuple column names: " + tupleColumnNames + ", relation column names: " + columnNames);
        }
        String pk = tuple.getPrimaryKeyValue();
        log.info("Inserting Tuple with PK=" + pk + " in Relation " + name);
        if (tuples.put(pk, tuple) != null) {
            log.info("Tuple with PK=" + pk + "Already exists.");
            return false;
        }
        tuple.state.setRelationChildCount(children.size());
        index.insertTuple(tuple);
        if (children.isEmpty()) {
            log.info("Relation " + name + " has no children, setting tuple " + pk + " alive");
            tuple.state.setAlive();
        } else {
            recursiveStatusUpdate(this, tuple, Action.INSERT);
        }
        return true;
    }

    public boolean deleteTuple(final String primaryKeyValue) {
        requireNonNull(primaryKeyValue);
        log.info("Deleting Tuple with PK=" + primaryKeyValue + " in Relation " + name);
        if (!tuples.containsKey(primaryKeyValue)) {
            return false;
        }
        Tuple tuple = tuples.get(primaryKeyValue);
        index.deleteTuple(tuple);
        recursiveStatusUpdate(this, tuple, Action.DELETE);
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
        if (parents.isEmpty()) {
            isRoot = true;
        } else {
            isRoot = false;
            this.parents = parents;
        }

        boolean isLeaf;
        if (children.isEmpty()) {
            isLeaf = true;
        } else {
            isLeaf = false;
            this.children = children;
        }
        structure = RelationStructure.getStructure(isRoot, isLeaf);
    }

    //Note: getters and setters not used in private methods

    /**
     * returns null if tuple doesn't exist
     */
    @Nullable
    private Tuple tupleWithForeignKey(String foreignKey, String fKvalue) {
        List<Tuple> result = index.getTuple(foreignKey, fKvalue);
        int size = (result == null) ? 0 : result.size();
        if (size > 1) {
            throw new RuntimeException(size + " tuples found with foreign key. Should be at most 1");
        }

        return size == 0 ? null : result.get(0);
    }

    private static void recursiveStatusUpdate(Relation relation, Tuple tuple, Action action) {
        log.info("Recursively updating " + relation.name + " for tuple " + tuple.getPrimaryKeyValue());
        Map<String, Relation> children = relation.children;
        updateChildTupleStatuses(children, relation, tuple, action);
        children.forEach((childName, child) -> {
            recursiveStatusUpdate(child, tuple, action);
        });

    }

    private static void updateChildTupleStatuses(Map<String, Relation> children, Relation parent, Tuple tuple, Action action) {
        children.forEach((childName, child) -> {
            String primaryKeyValue = tuple.getPrimaryKeyValue();
            log.info("Iterating over child " + childName + " for recursive update to process tuple with PK=" + primaryKeyValue);
            //Test only for development purposes. Ideally this check should be redundant as child must contain the PK of this table as an FK, that's why it is its child.
            String parentPK = parent.primaryKeyName;
            if (!child.columnNames.contains(parentPK)) {
                throw new RuntimeException("Child " + childName + " of " + parent.name + " doesn't have PK: " + parentPK + " as a foreign key.");
            }
            Tuple childTuple = child.tupleWithForeignKey(parentPK, primaryKeyValue);
            if (childTuple == null) {
                log.info("No child tuples found");
                return;
            }
            log.info("Found child tuple with PK=" + childTuple.getPrimaryKeyValue() + " containing FK=" + primaryKeyValue + " for column " + parentPK);
            if (action == Action.INSERT) {
                parent.tuples.get(primaryKeyValue).state.incrementState(child.name, primaryKeyValue);
            } else {
                parent.tuples.get(primaryKeyValue).state.decrementState(child.name, primaryKeyValue);
            }
        });
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
                entry.put(v.getValue(), tuple);
                index.put(k, entry);
            });
        }

        void deleteTuple(Tuple tuple) {
            tuple.getEntries().forEach((k, v) -> {
                index.get(k).remove(v, tuple);
            });
        }

        @Nullable
        List<Tuple> getTuple(String key, String value) {
            Multimap<String, Tuple> values = index.get(key);
            return (values == null) ? null : (List<Tuple>) values.get(value);
        }
    }

    private enum Action {
        INSERT,
        DELETE;
    }
}
