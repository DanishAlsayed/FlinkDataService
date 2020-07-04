package src.test.java;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import src.main.java.Relation;
import src.main.java.RelationStructure;
import src.main.java.Tuple;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class RelationTest {
    List<Relation> relations;

    @BeforeAll
    void setup() {
        relations = TestHelper.tpchSchema();
    }

    @Test
    public void allAlive() {
        assertAllAlive(relations, null);
    }

    @Test
    public void partialAliveTest() {
        assertAllAlive(relations, null);

        //Cannot use existing relations object due to lack of deep copy implementation i.e. Relation doesn't implement Cloneable
        List<Relation> relations1 = TestHelper.tpchSchema();
        Relation lineItem = relations1.get(0);
        assertEquals(lineItem.getName(), "lineItem");

        Relation orders = relations1.get(1);
        assertEquals(orders.getName(), "orders");

        Map<String, String> entries = new HashMap<>();
        entries.put("orderKey", "3");
        entries.put("customerKey", "3");
        assertTrue(orders.insertTuple(new Tuple("orders", "3", entries)));

        entries = new HashMap<>();
        entries.put("lineNumber", "3");
        entries.put("orderKey", "3");
        assertTrue(lineItem.insertTuple(new Tuple("lineItem", "3", entries)));

        assertAllAlive(relations1, "3");

        Relation customers = relations1.get(2);
        entries = new HashMap<>();
        entries.put("customerKey", "3");
        customers.insertTuple(new Tuple("customers", "3", entries));
        assertAllAlive(relations1, null);

    }

    @Test
    public void tupleDeleteFromLeaf() {
        assertAllAlive(relations, null);
        //Cannot use existing relations object due to lack of deep copy implementation i.e. Relation doesn't implement Cloneable
        List<Relation> relations1 = TestHelper.tpchSchema();
        Relation customers = relations1.get(2);
        assertTrue(customers.deleteTuple("1"));
        assertAllAlive(relations1, "1");

    }

    @Test
    public void tupleDeleteFromIntermediate() {
        assertAllAlive(relations, null);
        //Cannot use existing relations object due to lack of deep copy implementation i.e. Relation doesn't implement Cloneable
        List<Relation> relations1 = TestHelper.tpchSchema();
        Relation orders = relations1.get(1);
        assertTrue(orders.deleteTuple("1"));
        assertAllAlive(relations1, "1");
    }

    @Test
    public void tupleDeleteFromRoot() {
        assertAllAlive(relations, null);
        //Cannot use existing relations object due to lack of deep copy implementation i.e. Relation doesn't implement Cloneable
        List<Relation> relations1 = TestHelper.tpchSchema();
        Relation lineItem = relations1.get(0);
        assertTrue(lineItem.deleteTuple("1"));
        assertAllAlive(relations1, null);
    }

    private void assertAllAlive(List<Relation> relations, String except) {
        relations.forEach(relation -> relation.getTuples().forEach((k, v) -> {
            System.out.println(relation.getName() + " " + k + "->" + v.getPrimaryKeyValue() + " isAlive()? " + v.isAlive());
            if (v.getPrimaryKeyValue().equals(except) && relation.getStructure() != RelationStructure.LEAF) {
                assertFalse(v.isAlive());
            } else {
                assertTrue(v.isAlive());
            }
        }));
    }
}
