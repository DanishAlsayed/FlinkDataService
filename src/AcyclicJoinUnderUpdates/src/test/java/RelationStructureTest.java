package src.test.java;

import org.junit.jupiter.api.Test;
import src.main.java.RelationStructure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static src.main.java.RelationStructure.*;

public class RelationStructureTest {
    @Test
    public void getStructure() {
        assertEquals(RelationStructure.getStructure(true, true), ONLY);
        assertEquals(RelationStructure.getStructure(true, false), ROOT);
        assertEquals(RelationStructure.getStructure(false, true), LEAF);
        assertEquals(RelationStructure.getStructure(false, false), INTERMEDIATE);
    }
}
