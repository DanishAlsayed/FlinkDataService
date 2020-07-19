import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import src.main.java.Relation;
import src.main.java.Tuple;

import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public class TPCHQuery3Process extends ProcessFunction<Tuple, Relation> implements QueryProcess {

    private final Map<String, Relation> relationsMap;
    private final Date CUTOFF_DATE;
    private final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");

    public TPCHQuery3Process(List<Relation> relationsMap) {
        this.relationsMap = populateRelationsMap(relationsMap);
        try {
            this.CUTOFF_DATE = dateFormatter.parse("1995-03-15");
        } catch (ParseException e) {
            throw new RuntimeException("Unable to parse date ", e);
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void processElement(Tuple tuple, Context context, Collector<Relation> collector) {
        //TODO:
        // Insert tuple in its relation
        // Calculate update
        // Collect update
        relationsMap.get(tuple.getRelationName()).insertTuple(tuple);
        //TODO: do not collect if result is empty
        collector.collect(calculateResult(tuple));
    }

    @Override
    public Relation calculateResult(Tuple tuple) {
        Relation result = getResultRelation();
        String relationName = tuple.getRelationName();
        Relation relation = relationsMap.get(relationName);
        if (relationName.equals("lineitem")) {
            Tuple resultTuple = processLineItemTuple(tuple, relation);
            if (resultTuple == null) {
                return result;
            }
            result.insertTuple(resultTuple);
            return result;
        }
        if (relationName.equals("orders")) {
            Tuple resultTuple = processOrdersTuple(tuple, relation);
            if (resultTuple == null) {
                return result;
            }
            result.insertTuple(resultTuple);
            return result;
        }

        return result;
    }

    @Nullable
    private Tuple processOrdersTuple(Tuple tuple, Relation relation) {
        Relation.Index index = relation.getAliveTuplesIndex();
        List<Tuple> tuples = index.getTuple("custkey", tuple.getEntries().get("custkey").getValue());
        if (tuples == null || tuples.size() == 0) {
            return null;
        }
        Map<String, String> entries = new HashMap<>();
        String orderkey = tuple.getEntries().get("orderkey").getValue();
        entries.put("orderkey", orderkey);
        Relation lineitem = relationsMap.get("lineitem");
        Relation.Index lineitemIndex = lineitem.getAliveTuplesIndex();
        List<Tuple> lineitemTuples = lineitemIndex.getTuple("orderkey", orderkey);
        if (lineitemTuples == null || lineitemTuples.size() == 0) {
            return null;
        }
        entries.put("orderkey", orderkey);
        entries.put("revenue", String.valueOf(calculateRevenue(lineitemTuples)));
        entries.put("orderdate", tuple.getEntries().get("orderdate").getValue());
        entries.put("shippriority", tuple.getEntries().get("shippriority").getValue());

        return new Tuple("result", orderkey, entries);
    }

    @Nullable
    private Tuple processLineItemTuple(Tuple tuple, Relation relation) {
        Relation.Index index = relation.getAliveTuplesIndex();
        List<Tuple> tuples = index.getTuple("orderkey", tuple.getEntries().get("orderkey").getValue());
        if (tuples == null || tuples.size() == 0) {
            return null;
        }

        Map<String, String> entries = new HashMap<>();
        String orderkey = tuple.getEntries().get("orderkey").getValue();
        entries.put("orderkey", orderkey);
        entries.put("revenue", String.valueOf(calculateRevenue(tuples)));
        Relation orders = relationsMap.get("orders");
        entries.put("orderdate", orders.getTuples().get(orderkey).getEntries().get("orderdate").getValue());
        entries.put("shippriority", orders.getTuples().get(orderkey).getEntries().get("shippriority").getValue());

        return new Tuple("result", orderkey, entries);
    }

    private double calculateRevenue(List<Tuple> lineitemTuples) {
        double sum = 0;
        for (Tuple tuple : lineitemTuples) {
            double price = Double.parseDouble(tuple.getEntries().get("extendedprice").getValue());
            double discount = Double.parseDouble(tuple.getEntries().get("discount").getValue());
            sum += price * (1 - discount);
        }
        return sum;
    }

    private Relation getResultRelation() {
        Relation result = new Relation("result", "orderkey", asList("orderkey", "revenue", "orderdate", "shippriority"));
        result.setParents(new HashMap<>());
        result.setChildren(new HashMap<>());
        return result;
    }

    private Map<String, Relation> populateRelationsMap(final List<Relation> relations) {
        requireNonNull(relations);
        if (relations.size() != 3) {
            throw new RuntimeException("Expected 3 relations, got " + relations.size());
        }
        Map<String, Relation> result = new HashMap<>();
        relations.forEach(relation -> {
            result.put(relation.getName(), relation);
        });
        return result;
    }
}
