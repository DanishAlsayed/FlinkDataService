package src.test.java;

import src.main.java.Relation;
import src.main.java.Tuple;

import java.util.*;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;

public class TestHelper {

    static List<Relation> figure5Setup() {
        Relation r1 = new Relation("R1", "x1", new HashSet<>(singleton("x1")));
        Relation r2 = new Relation("R2", "x2", new HashSet<>(asList("x1", "x2")));
        Relation r3 = new Relation("R3", "x3", new HashSet<>(asList("x1", "x3")));
        //Note: Figure 5 in the paper actually doesn't indicate the primary key of R4
        Relation r4 = new Relation("R4", "x4", new HashSet<>(asList("x2", "x3")));

        Map<String, Relation> children = new HashMap<>();
        Map<String, Relation> parents = new HashMap<>();
        Map<String, String> entries = new HashMap<>();

        //Populate R1
        r1.insertTuple(new Tuple("R1", 0, 2, "1", singletonMap("x1", "1")));
        r1.insertTuple(new Tuple("R1", 0, 2, "2", singletonMap("x1", "2")));

        //Populate R2
        entries.put("x1", "1");
        entries.put("x2", "1");
        r2.insertTuple(new Tuple("R2", 1, 1, "1", entries));
        entries.put("x1", "2");
        entries.put("x2", "2");
        r2.insertTuple(new Tuple("R2", 1, 1, "2", entries));
        children.put("R1", r1);
        parents.put("R4", r4);
        r2.populateFamily(parents, children);

        //Populate R3
        entries = new HashMap<>();
        children = new HashMap<>();
        parents = new HashMap<>();
        entries.put("x1", "1");
        entries.put("x3", "1");
        r3.insertTuple(new Tuple("R3", 1, 1, "1", entries));
        entries.put("x1", "2");
        entries.put("x3", "2");
        r3.insertTuple(new Tuple("R3", 1, 1, "2", entries));
        children.put("R1", r1);
        parents.put("R4", r4);
        r3.populateFamily(parents, children);

        //Populate R4
        entries = new HashMap<>();
        children = new HashMap<>();
        entries.put("x4", "1");
        entries.put("x2", "1");
        entries.put("x3", "1");
        r4.insertTuple(new Tuple("R4", 2, 0, "1", entries));
        entries.put("x4", "2");
        entries.put("x2", "2");
        entries.put("x3", "2");
        r4.insertTuple(new Tuple("R4", 2, 0, "2", entries));
        children.put("R2", r2);
        children.put("R3", r3);
        r4.setChildren(children);

        List<Relation> result = new ArrayList<>();
        result.add(r1);
        result.add(r2);
        result.add(r3);
        result.add(r4);
        return result;
    }
}
