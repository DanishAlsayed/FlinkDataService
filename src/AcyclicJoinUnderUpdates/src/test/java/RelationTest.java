package src.test.java;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import src.main.java.Relation;
import src.main.java.Tuple;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class RelationTest {
    private Relation r1;
    private Relation r2;
    private Relation r3;
    private Relation r4;

    @BeforeAll
    void setup() {
        List<Relation> relations = TestHelper.figure5Setup();
        r1 = relations.get(0);
        r2 = relations.get(1);
        r3 = relations.get(2);
        r4 = relations.get(3);
    }
    @Test
    public void fig5RelationsTest() {
        Map<String, Relation> children, parents;

        //R1
        structureTest(r1, 0, 0, 2);
        Map<String, Tuple> tuples = r1.getTuples();
        assertEquals(tuples.get("1").getPrimaryKeyValue(), "1");
        assertEquals(tuples.get("2").getPrimaryKeyValue(), "2");

        //R2
        structureTest(r2, 1, 1, 2);
        children = r2.getChildren();
        assertEquals(children.get("R1"), r1);
        parents = r2.getParents();
        assertEquals(parents.get("R4"), r4);
        tuples = r2.getTuples();
        assertEquals(tuples.get("1").getPrimaryKeyValue(), "1");
        assertEquals(tuples.get("2").getPrimaryKeyValue(), "2");

        //R3
        structureTest(r3, 1, 1, 2);
        children = r3.getChildren();
        assertEquals(children.get("R1"), r1);
        parents = r3.getParents();
        assertEquals(parents.get("R4"), r4);
        tuples = r3.getTuples();
        assertEquals(tuples.get("1").getPrimaryKeyValue(), "1");
        assertEquals(tuples.get("2").getPrimaryKeyValue(), "2");

        //R4
        structureTest(r4, 2, 0, 2);
        children = r4.getChildren();
        assertEquals(children.get("R2"), r2);
        assertEquals(children.get("R3"), r3);
        tuples = r3.getTuples();
        assertEquals(tuples.get("1").getPrimaryKeyValue(), "1");
        assertEquals(tuples.get("2").getPrimaryKeyValue(), "2");
    }

    private void structureTest(Relation relation, int numOfChild, int numOfParent, int numOfTuples) {
        assertEquals(relation.numberOfChildren(), numOfChild);
        assertEquals(relation.numberOfParents(), numOfParent);
        assertEquals(relation.getTuples().size(), numOfTuples);
    }
}
