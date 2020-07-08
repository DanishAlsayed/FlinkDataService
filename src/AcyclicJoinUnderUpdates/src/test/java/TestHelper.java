package src.test.java;

import src.main.java.Relation;
import src.main.java.Tuple;

import java.util.*;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHelper {

    static List<Relation> tpchSchema() {
        Relation lineItem = new Relation("lineItem", "lineNumber", asList("lineNumber", "orderKey"));
        Relation orders = new Relation("orders", "orderKey", asList("orderKey", "customerKey"));
        Relation customers = new Relation("customers", "customerKey", singletonList("customerKey"));

        //Populating structure
        lineItem.setChildren(singletonMap("orders", orders));
        orders.setParents(singletonMap("lineItem", lineItem));
        orders.setChildren(singletonMap("customers", customers));
        customers.setParents(singletonMap("orders", orders));

        //Populating rows
        Map<String, String> entries = new HashMap<>();
        entries.put("orderKey", "1");
        entries.put("customerKey", "1");
        assertTrue(orders.insertTuple(new Tuple("orders", "1", entries)));
        entries = new HashMap<>();
        entries.put("orderKey", "2");
        entries.put("customerKey", "2");
        assertTrue(orders.insertTuple(new Tuple("orders", "2", entries)));

        entries = new HashMap<>();
        entries.put("lineNumber", "1");
        entries.put("orderKey", "1");
        assertTrue(lineItem.insertTuple(new Tuple("lineItem", "1", entries)));
        entries = new HashMap<>();
        entries.put("lineNumber", "2");
        entries.put("orderKey", "2");
        assertTrue(lineItem.insertTuple(new Tuple("lineItem", "2", entries)));

        entries = new HashMap<>();
        entries.put("customerKey", "1");
        assertTrue(customers.insertTuple(new Tuple("customers", "1", entries)));
        entries = new HashMap<>();
        entries.put("customerKey", "2");
        assertTrue(customers.insertTuple(new Tuple("customers", "2", entries)));

        return asList(lineItem, orders, customers);
    }
}
