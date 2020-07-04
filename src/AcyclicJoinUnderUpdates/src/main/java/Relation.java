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
        tupleStatusUpdates(this, tuple, Action.INSERT);
        return true;
    }

    public boolean deleteTuple(final String primaryKeyValue) {
        requireNonNull(primaryKeyValue);
        log.info("Deleting Tuple with PK=" + primaryKeyValue + " in Relation " + name);
        Tuple tuple = tuples.remove(primaryKeyValue);
        if (tuple == null) {
            return false;
        }
        tupleStatusUpdates(this, tuple, Action.DELETE);
        index.deleteTuple(tuple);
        return true;
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

    /**
     * This is STRICTLY for a relation tree of the structure ROOT -> INTERMEDIATE -> LEAF. Corresponding to lineintem, orders and customer tables only in the TPC-H schema.
     *
     * @param relation
     * @param tuple
     */
    private static void tupleStatusUpdates(final Relation relation, Tuple tuple, Action action) {
        if (action == Action.DELETE) {
            if (relation.structure == RelationStructure.INTERMEDIATE) {
                decrementParentStatus(relation, tuple);
            } else if (relation.structure == RelationStructure.LEAF) {
                Relation relation1 = relation;
                for (int i = 0; i < 2; i++) {
                    relation1 = decrementParentStatus(relation1, tuple);
                    if (relation1 == null) {
                        break;
                    }
                }
            }
        } else if (action == Action.INSERT) {
            updateSelf(relation, tuple);

            if (relation.structure == RelationStructure.INTERMEDIATE) {
                updateParent(relation, tuple);
            } else if (relation.structure == RelationStructure.LEAF) {
                Relation relation1 = relation;
                for (int i = 0; i < 2; i++) {
                    relation1 = updateParent(relation1, tuple);
                    if (relation1 == null) {
                        break;
                    }
                }
            }
        } else {
            throw new IllegalArgumentException("Unknown action: " + action);
        }
    }

    private static Relation decrementParentStatus(final Relation relation, Tuple tuple) {
        Tuple parentTuple = retrieveParentTuple(relation, tuple);
        if (parentTuple == null) {
            return null;
        }
        Relation parent = relation.parents.entrySet().iterator().next().getValue();
        parentTuple.state.decrementState(parent.name, parentTuple.getPrimaryKeyValue());
        return parent;
    }

    @Nullable
    private static Relation updateParent(final Relation relation, Tuple tuple) {
        Tuple parentTuple = retrieveParentTuple(relation, tuple);
        if (parentTuple == null) {
            return null;
        }
        //Note: we know that there is only 1 parent
        Relation parent = relation.parents.entrySet().iterator().next().getValue();
        updateSelf(parent, parentTuple);
        return parent;
    }

    private static void updateSelf(final Relation relation, Tuple tuple) {
        log.info("Updating status for tuple with PK: " + relation.primaryKeyName + "=" + tuple.getPrimaryKeyValue());
        if (relation.structure == RelationStructure.LEAF) {
            tuple.state.setAlive();
            return;
        }
        relation.children.forEach((childName, child) -> {
            String childPK = child.primaryKeyName;
            String tValue = tuple.getEntries().get(childPK).getValue();
            List<Tuple> cTuples = child.index.getTuple(childPK, tValue);
            if (cTuples == null) {
                return;
            }
            if (cTuples.size() != 1) {
                log.info("Number of child tuples found " + cTuples.size() + " from " + child.name + " for PK: " + childPK + "=" + tValue);
                return;
            }
            Tuple cTuple = cTuples.get(0);
            if (cTuple.getPrimaryKeyValue().equals(tValue) && cTuple.isAlive()) {
                tuple.state.incrementState(relation.name, tuple.getPrimaryKeyValue());
            }
        });
    }

    @Nullable
    private static Tuple retrieveParentTuple(Relation relation, Tuple tuple) {
        Map<String, Relation> parents = relation.parents;
        if (parents.size() != 1) {
            throw new RuntimeException("Expected 1 and only 1 parent for relation " + relation.name + " with structure " + relation.structure);
        }
        Relation parent = parents.entrySet().iterator().next().getValue();
        List<Tuple> tuples = parent.index.getTuple(relation.primaryKeyName, tuple.getPrimaryKeyValue());
        if (tuples == null) {
            return null;
        }
        if (tuples.size() != 1) {
            log.info("Number of parent tuples found " + tuples.size() + " from " + parent.name + " for FK: " + relation.primaryKeyName + "=" + tuple.getPrimaryKeyValue());
            return null;
        }

        return tuples.get(0);
    }

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
