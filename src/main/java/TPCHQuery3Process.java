import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import src.main.java.Relation;
import src.main.java.Tuple;

import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public class TPCHQuery3Process extends ProcessFunction<Tuple, List<Tuple>> implements QueryProcess {

    private final Map<String, Relation> relationsMap;

    public TPCHQuery3Process(List<Relation> relationsMap) {
        this.relationsMap = populateRelationsMap(relationsMap);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void processElement(Tuple tuple, Context context, Collector<List<Tuple>> collector) {
        relationsMap.get(tuple.getRelationName()).insertTuple(tuple);

        List<Tuple> result = calculateResult(tuple);
        if (result != null) {
            collector.collect(result);
        }
    }

    @Nullable
    @Override
    public List<Tuple> calculateResult(Tuple tuple) {
        String relationName = tuple.getRelationName();
        List<Tuple> resultTuples;
        if (relationName.equals("lineitem")) {
            resultTuples = processLineItemTuple(tuple);
            return (resultTuples == null) ? null : groupByAndSumRevenue(resultTuples);
        }
        if (relationName.equals("orders")) {
            resultTuples = processOrdersTuple(tuple);
            return (resultTuples == null) ? null : groupByAndSumRevenue(resultTuples);
        }
        if (relationName.equals("customer")) {
            resultTuples = processCustomerTuple(tuple);
            return (resultTuples == null) ? null : groupByAndSumRevenue(resultTuples);
        }

        throw new RuntimeException("Unknown relation name");
    }

    private List<Tuple> groupByAndSumRevenue(List<Tuple> resultTuples) {
        Map<Object, Map<Object, Map<Object, Double>>> resultMap = resultTuples.stream().collect(Collectors.groupingBy(tuple -> {
            return tuple.getEntries().get("orderkey").getValue();
        }, Collectors.groupingBy(tuple -> {
            return tuple.getEntries().get("orderdate").getValue();
        }, Collectors.groupingBy(tuple -> {
            return tuple.getEntries().get("shippriority").getValue();
        }, Collectors.summingDouble(tuple -> {
            return Double.parseDouble(tuple.getEntries().get("revenue").getValue());
        })))));

        List<Tuple> resultList = new ArrayList<>();
        for (Map.Entry<Object, Map<Object, Map<Object, Double>>> entry : resultMap.entrySet()) {
            String orderkey = (String) entry.getKey();
            String orderdate = (String) entry.getValue().entrySet().iterator().next().getKey();
            String shippriority = (String) entry.getValue().entrySet().iterator().next().getValue().entrySet().iterator().next().getKey();
            double revenue = entry.getValue().entrySet().iterator().next().getValue().entrySet().iterator().next().getValue();
            Map<String, String> entries = new HashMap<>();
            entries.put("orderkey", orderkey);
            entries.put("orderdate", orderdate);
            entries.put("shippriority", shippriority);
            entries.put("revenue", String.valueOf(revenue));
            resultList.add(new Tuple("result", orderkey, entries));
        }
        System.out.println(resultMap.size());

        return resultList;
    }

    @Nullable
    private List<Tuple> processCustomerTuple(Tuple insertedTuple) {
        List<Tuple> ordersLiveTuples = relationsMap.get("orders").getAliveTuplesIndex().getTuple("custkey", insertedTuple.getPrimaryKeyValue());
        if (ordersLiveTuples == null || ordersLiveTuples.size() == 0) {
            return null;
        }
        List<Tuple> lineitemLiveTuples = new ArrayList<>();
        Relation.Index lineItemLiveIndex = relationsMap.get("lineitem").getAliveTuplesIndex();
        for (Tuple orderTuple : ordersLiveTuples) {
            List<Tuple> lineitemTuples = lineItemLiveIndex.getTuple("orderkey", orderTuple.getPrimaryKeyValue());
            if (lineitemTuples == null || lineitemTuples.size() == 0) {
                continue;
            }
            lineitemLiveTuples.addAll(lineitemTuples);
        }

        if (lineitemLiveTuples.size() == 0) {
            return null;
        }

        return getResultTuples(lineitemLiveTuples);
    }

    @Nullable
    private List<Tuple> processOrdersTuple(Tuple insertedTuple) {
        String orderkey = insertedTuple.getPrimaryKeyValue();
        List<Tuple> lineitemLiveTuples = getLineitemLiveTuples(orderkey);
        if (lineitemLiveTuples == null || lineitemLiveTuples.size() == 0) {
            return null;
        }

        return getResultTuples(lineitemLiveTuples);
    }

    @Nullable
    private List<Tuple> processLineItemTuple(Tuple insertedTuple) {
        String orderkey = insertedTuple.getEntries().get("orderkey").getValue();
        List<Tuple> lineitemLiveTuples = getLineitemLiveTuples(orderkey);
        if (lineitemLiveTuples == null || lineitemLiveTuples.size() == 0) {
            return null;
        }

        return getResultTuples(lineitemLiveTuples);
    }

    private List<Tuple> getResultTuples(List<Tuple> lineitemLiveTuples) {
        List<Tuple> result = new ArrayList<>();
        for (Tuple tuple : lineitemLiveTuples) {
            Map<String, String> entries = new HashMap<>();
            String orderkey = tuple.getEntries().get("orderkey").getValue();
            entries.put("orderkey", orderkey);
            //TODO: fix revenue calculation, do it after groupby
            double price = Double.parseDouble(tuple.getEntries().get("extendedprice").getValue());
            double discount = Double.parseDouble(tuple.getEntries().get("discount").getValue());
            entries.put("revenue", String.valueOf(price * (1 - discount)));
            Relation orders = relationsMap.get("orders");
            entries.put("orderdate", orders.getTuples().get(orderkey).getEntries().get("orderdate").getValue());
            entries.put("shippriority", orders.getTuples().get(orderkey).getEntries().get("shippriority").getValue());
            result.add(new Tuple("result", orderkey, entries));
        }

        return result;
    }

    private List<Tuple> getLineitemLiveTuples(String orderkey) {
        return relationsMap.get("lineitem").getAliveTuplesIndex().getTuple("orderkey", orderkey);
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
