package src.main.java;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;

/**
 * A class that will construct the schema i.e. populate Relations and their structures (and return the root Relation?)
 */
public class SchemaBuilder {
    /**
     * returns a list of relations needed for query 3 of the TPC-H schema in order top to bottom as per the foreign key graph
     *
     * @return list of relations
     */
    public static List<Relation> query3Schema() {
        Relation lineItem = new Relation("lineitem", "linenumber", asList("shipdate", "orderkey", "discount", "extendedprice", "linenumber"));
        Relation orders = new Relation("orders", "orderkey", asList("orderdate", "orderkey", "custkey", "orderpriority", "shippriority"));
        Relation customer = new Relation("customer", "custkey", asList("custkey", "mktsegment"));

        //Populating structure
        lineItem.setChildren(singletonMap("orders", orders));
        orders.setParents(singletonMap("lineitem", lineItem));
        orders.setChildren(singletonMap("customer", customer));
        customer.setParents(singletonMap("orders", orders));

        List<Relation> relations = new ArrayList<>();
        relations.add(lineItem);
        relations.add(orders);
        relations.add(customer);
        return relations;
    }
}
