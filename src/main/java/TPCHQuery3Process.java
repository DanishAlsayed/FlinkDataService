import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import src.main.java.Relation;
import src.main.java.Tuple;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public class TPCHQuery3Process extends ProcessFunction<Tuple, Relation> implements QueryProcess {

    private final Map<String, Relation> relations;
    private final Date CUTOFF_DATE;
    private final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");

    public TPCHQuery3Process(List<Relation> relations) {
        this.relations = populateRelations(relations);
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
        if (!isValidTuple(tuple)) {
            collector.collect(getResultRelation());
            return;
        }
        System.out.println(tuple.getRelationName() + "=>" + tuple.toString());
        relations.get(tuple.getRelationName()).insertTuple(tuple);
        collector.collect(calculateResult());
    }

    @Override
    public Relation calculateResult() {
        Relation result = getResultRelation();

        return result;
    }

    private Relation getResultRelation() {
        return new Relation("result", "orderkey", asList("orderkey", "revenue", "orderdate", "shippriority"));
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

    private boolean isValidTuple(Tuple tuple) {
        boolean dateIsValid = true;
        if (tuple.getRelationName().equals("orders")) {
            dateIsValid = isValidDate(tuple, "orderdate", Operator.LESS_THAN);
        }
        if (tuple.getRelationName().equals("lineitem")) {
            dateIsValid = isValidDate(tuple, "shipdate", Operator.GREATER_THAN);
        }

        boolean validMarketSegment = true;
        if (tuple.getRelationName().equals("customer")) {
            validMarketSegment = isValidMarketSegment(tuple);
        }
        return dateIsValid && validMarketSegment;
    }

    private boolean isValidDate(Tuple tuple, String dateColumn, Operator operator) {
        try {
            Date date = dateFormatter.parse(tuple.getEntries().get(dateColumn).getValue());
            return operator.apply(date, CUTOFF_DATE);
        } catch (ParseException e) {
            throw new RuntimeException("Unable to parse tuple date ", e);
        }
    }

    private boolean isValidMarketSegment(Tuple customerTuple) {
        return customerTuple.getEntries().get("mktsegment").getValue().equals("BUILDING");
    }

    private enum Operator {
        GREATER_THAN(">") {
            @Override
            public boolean apply(Date d1, Date d2) {
                return d1.after(d2);
            }
        },
        LESS_THAN("<") {
            @Override
            public boolean apply(Date d1, Date d2) {
                return d1.before(d2);
            }
        };

        private final String text;

        private Operator(String text) {
            this.text = text;
        }

        public abstract boolean apply(Date d1, Date d2);

        @Override
        public String toString() {
            return text;
        }
    }
}
