package src.main.java;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.jetbrains.annotations.Nullable;
//import com.sun.istack.internal.logging.Logger;

import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.StringUtils.isEmpty;

import java.io.Serializable;
import java.util.*;

import static java.util.Objects.requireNonNull;

public class Relation implements Serializable {
    //private static Logger log = Logger.getLogger(Relation.class);
    private final String name;
    private final Set<String> columnNamesSet;
    private final List<String> columnNamesList;
    private final Map<String, Tuple> tuples;
    private final String primaryKeyName;
    private Map<String, Relation> parents;
    private Map<String, Relation> children;
    private RelationStructure structure;
    private Index generalIndex;
    private Index aliveTuplesIndex;
    //TODO: consider having a Map of foreign keys, key= FK name & value=Relation

    /**
     * Class representing a table. First an empty table should be created, then the family structure should be populated followed by populating tuples. This order of construction is necessary.
     * DAG condition is PK->FK, opposite to the paper, same as the presentation.
     *
     * @param name
     * @param primaryKeyName
     */
    public Relation(final String name, final String primaryKeyName, final List<String> columnNamesList) {
        if (isEmpty(primaryKeyName) || isEmpty(name)) {
            throw new RuntimeException("Primary key name & relation name cannot be empty or null." +
                    " name = " + (name == null ? "null" : name) + ", primaryKeyName = " + (primaryKeyName == null ? "null" : primaryKeyName));
        }

        this.primaryKeyName = primaryKeyName;
        this.name = name;
        tuples = new HashMap<>();
        parents = new HashMap<>();
        children = new HashMap<>();
        requireNonNull(columnNamesList);
        if (columnNamesList.size() < 1) {
            throw new RuntimeException("A relation must have at least 1 column. Provided columnNames' size is: " + columnNamesList.size());
        }
        //this.columnNamesSet = new HashSet<>(columnNamesList);
        this.columnNamesSet = new HashSet<>();
        columnNamesList.forEach(c_name -> {
            columnNamesSet.add(c_name.intern());
        });
        if (this.columnNamesSet.size() != columnNamesList.size()) {
            throw new RuntimeException("Duplicate column names found: " + columnNamesList);
        }
        this.columnNamesList = columnNamesList;
        //TODO: do we need the tuples map if we have index?
        generalIndex = new Index();
        aliveTuplesIndex = new Index();
    }

    public boolean insertTuple(Tuple tuple) {
        requireNonNull(tuple);
        Set<String> tupleColumnNames = tuple.getEntries().keySet();
        if (tupleColumnNames.size() != columnNamesSet.size() || !tupleColumnNames.containsAll(columnNamesSet)) {
            throw new RuntimeException("Column names for tuple and relation do not match. Tuple column names: " + tupleColumnNames + ", relation column names: " + columnNamesSet);
        }
        String pk = tuple.getPrimaryKeyValue();
        tuples.put(pk, tuple);
        tuple.state.setRelationChildCount(children.size());
        generalIndex.insertTuple(tuple);
        tupleStatusUpdates(this, tuple, Action.INSERT);
        return true;
    }

    public boolean deleteTuple(final String primaryKeyValue) {
        requireNonNull(primaryKeyValue);
        //log.info("Deleting Tuple with PK=" + primaryKeyValue + " in Relation " + name);
        Tuple tuple = tuples.remove(primaryKeyValue);
        if (tuple == null) {
            return false;
        }
        tupleStatusUpdates(this, tuple, Action.DELETE);
        generalIndex.deleteTuple(tuple);
        aliveTuplesIndex.deleteTuple(tuple);
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

    public Index getGeneralIndex() {
        return generalIndex;
    }

    public Index getAliveTuplesIndex() {
        return aliveTuplesIndex;
    }

    public RelationStructure getStructure() {
        return structure;
    }

    public Set<String> getColumnNamesSet() {
        return columnNamesSet;
    }

    public List<String> getColumnNamesList() {
        return columnNamesList;
    }

    public String getName() {
        return name;
    }

    public String getPrimaryKeyName() {
        return primaryKeyName;
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
     * This is STRICTLY for a relation tree of the structure ROOT -> INTERMEDIATE -> LEAF. Corresponding to lineintem, orders and customer tables only in the TPC-H schema.
     *
     * @param relation
     * @param tuple
     */
    private static void tupleStatusUpdates(final Relation relation, Tuple tuple, Action action) {
        if (relation.structure == RelationStructure.ONLY) {
            return;
        }
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
                List<Tuple> parentTuples = retrieveParentTuples(relation, singletonList(tuple));
                updateParent(relation, parentTuples);
            } else if (relation.structure == RelationStructure.LEAF) {
                Relation relation1 = relation;
                List<Tuple> tuples = singletonList(tuple);
                for (int i = 0; i < 2; i++) {
                    tuples = retrieveParentTuples(relation1, tuples);
                    relation1 = updateParent(relation1, tuples);
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
        List<Tuple> parentTuples = retrieveParentTuples(relation, singletonList(tuple));
        Relation parent = relation.parents.entrySet().iterator().next().getValue();
        parentTuples.forEach(parentTuple -> parentTuple.state.decrementState(parent.name, parentTuple.getPrimaryKeyValue()));
        return parent;
    }

    @Nullable
    private static Relation updateParent(final Relation relation, List<Tuple> parentTuples) {
        if (parentTuples == null) {
            return null;
        }
        //Note: we know that there is only 1 parent
        Relation parent = relation.parents.entrySet().iterator().next().getValue();
        parentTuples.forEach(parentTuple -> updateSelf(parent, parentTuple));

        return parent;
    }

    private static void updateSelf(final Relation relation, Tuple tuple) {
        //log.info("Updating status for tuple with PK: " + relation.primaryKeyName + "=" + tuple.getPrimaryKeyValue());
        if (relation.structure == RelationStructure.LEAF) {
            tuple.state.setAlive();
            relation.aliveTuplesIndex.insertTuple(tuple);
            //relation.generalIndex.deleteTuple(tuple);
            return;
        }
        Set<Map.Entry<String, Relation>> entrySet = relation.getChildren().entrySet();
        if (entrySet.size() != 1) {
            throw new RuntimeException("Expected exactly 1 child for " + relation.getName() + ", got " + entrySet.size());
        }

        Relation child = entrySet.iterator().next().getValue();
        String childPK = child.primaryKeyName;
        String tValue = tuple.getEntries().get(childPK).getValue();
        List<Tuple> cTuples = child.generalIndex.getTuple(childPK, tValue);
        if (cTuples == null) {
            return;
        }
        if (cTuples.size() != 1) {
            //log.info("Number of child tuples found " + cTuples.size() + " from " + child.name + " for PK: " + childPK + "=" + tValue);
            return;
        }
        Tuple cTuple = cTuples.get(0);
        if (cTuple.getPrimaryKeyValue().equals(tValue) && cTuple.isAlive()) {
            tuple.state.incrementState(relation.name, tuple.getPrimaryKeyValue());
            //Note: we know that there is one and only one child so we can safely insert the tuple in the alive index
            relation.aliveTuplesIndex.insertTuple(tuple);
            //relation.generalIndex.deleteTuple(tuple);
        }
    }

    @Nullable
    private static List<Tuple> retrieveParentTuples(Relation relation, List<Tuple> tuples) {
        Map<String, Relation> parents = relation.parents;
        if (parents.size() != 1) {
            throw new RuntimeException("Expected 1 and only 1 parent for relation " + relation.name + " with structure " + relation.structure);
        }
        Relation parent = parents.entrySet().iterator().next().getValue();

        List<Tuple> parentTuples = new ArrayList<>();
        tuples.forEach(tuple -> parentTuples.addAll(parent.generalIndex.getTuple(relation.primaryKeyName, tuple.getPrimaryKeyValue())));
        /*if (parentTuples == null) {
            return null;
        }
        //TODO: this check prevents from updating live status if there are multiple tuples in the parent with the same PK of the child
        // which is possible as that PK is FK in the parent and thus can be repeated
        if (tuples.size() != 1) {
            //log.info("Number of parent tuples found " + tuples.size() + " from " + parent.name + " for FK: " + relation.primaryKeyName + "=" + tuples.getPrimaryKeyValue());
            return null;
        }*/

        return parentTuples;
    }

    public static class Index implements Serializable {
        Map<String, Multimap<String, Tuple>> index;

        Index() {
            index = new HashMap<>();
        }

        public Map<String, Multimap<String, Tuple>> getIndex() {
            return index;
        }

        /*public List<Tuple> getIndexTuples() {
            List<>
        }*/

        private void insertTuple(Tuple tuple) {
            tuple.getEntries().forEach((columnName, columnValue) -> {
                Multimap<String, Tuple> entry = index.get(columnName);//index.computeIfAbsent(columnName, e -> ArrayListMultimap.create());
                if (entry == null) {
                    entry = ArrayListMultimap.create();
                }
                entry.put(columnValue.getValue(), tuple);
                //This leads to having one mulitmap per column name
                index.put(columnName, entry);
            });
        }

        private void deleteTuple(Tuple tuple) {
            tuple.getEntries().forEach((k, v) -> {
                index.get(k).remove(v, tuple);
            });
        }

        public List<Tuple> getTuple(String key, String value) {
            Multimap<String, Tuple> values = index.get(key);
            return (values == null) ? new ArrayList<Tuple>() : (List<Tuple>) values.get(value);
        }
    }

    private enum Action {
        INSERT,
        DELETE;
    }
}
