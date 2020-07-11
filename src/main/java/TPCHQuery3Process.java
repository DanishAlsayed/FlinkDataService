import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import src.main.java.Relation;
import src.main.java.Tuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public class TPCHQuery3Process extends ProcessFunction<Tuple, Relation> implements QueryProcess {

    private final Map<String, Relation> relations;

    public TPCHQuery3Process(List<Relation> relations) {
        this.relations = populateRelations(relations);
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
        System.out.println("processElement=>" + tuple.toString());
        relations.get(tuple.getRelationName()).insertTuple(tuple);
        collector.collect(calculateResult());
    }

    private Map<String, Relation> populateRelations(final List<Relation> relations) {
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

    @Override
    public Relation calculateResult() {
        Relation result = new Relation("result", "orderkey", asList("orderkey", "revenue", "orderdate", "shippriority"));

        return result;
    }
}
