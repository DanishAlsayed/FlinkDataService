package src.main.java;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;

/**
 * A class that will construct the schema i.e. populate Relations and their structures (and return the root Relation?)
 */
public class SchemaBuilder {
    /**
     * returns a list of relations needed for query 3 of the TPC-H schema in order top to bottom as per the foreign key graph
     *
     * @return
     */
    public static List<Relation> query3Schema() {
        Relation lineItem = new Relation("lineItem", "lineNumber", new HashSet<>(asList("lineNumber", "orderKey")));
        Relation orders = new Relation("orders", "orderKey", new HashSet<>(asList("orderKey", "customerKey")));
        Relation customers = new Relation("customers", "customerKey", new HashSet<>(singleton("customerKey")));

        //Populating structure
        lineItem.setChildren(singletonMap("orders", orders));
        orders.setParents(singletonMap("lineItem", lineItem));
        orders.setChildren(singletonMap("customers", customers));
        customers.setParents(singletonMap("orders", orders));

        List<Relation> relations = new ArrayList<>();
        relations.add(lineItem);
        relations.add(orders);
        relations.add(customers);
        return relations;
    }
}
