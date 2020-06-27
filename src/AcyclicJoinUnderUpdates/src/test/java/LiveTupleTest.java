package src.test.java;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import src.main.java.Relation;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

import java.util.List;

@TestInstance(PER_CLASS)
public class LiveTupleTest {
    List<Relation> fig5Relations;

    @BeforeAll
    void setup() {
        fig5Relations = TestHelper.figure5Setup();
    }

    @Test
    public void fig5LiveTupleTest() {
        fig5Relations.forEach(relation -> relation.getTuples().forEach((k, v) -> {
            System.out.println(relation.getName() + " " + k + "->" + v.getPrimaryKeyValue() + " isAlive()? " + v.isAlive());
            //assertTrue(v.isAlive());
        }));
    }

}
