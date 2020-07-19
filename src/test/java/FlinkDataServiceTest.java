import org.junit.jupiter.api.Test;
import src.main.java.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

public class FlinkDataServiceTest {

    @Test
    void frontToBackTest() throws Exception {
        FlinkDataService fds = new FlinkDataService(asList("/home/danishalsayed/Desktop/FlinkProject/FlinkDataService/src/AcyclicJoinUnderUpdates/src/main/resources/data/lineitem_trimmed.csv",
                "/home/danishalsayed/Desktop/FlinkProject/FlinkDataService/src/AcyclicJoinUnderUpdates/src/main/resources/data/orders_trimmed.csv",
                "/home/danishalsayed/Desktop/FlinkProject/FlinkDataService/src/AcyclicJoinUnderUpdates/src/main/resources/data/customer_trimmed.csv"
        ));
        fds.fds();
    }

    @Test
    void testGroupBy() {
        List<Tuple> tuples = new ArrayList<>();

        Map<String, String> entries = new HashMap<>();
        entries.put("orderkey", "1");
        entries.put("orderdate", "1.1");
        entries.put("shippriority", "A");
        entries.put("revenue", "5.1");
        tuples.add(new Tuple("result", "1", entries));

        entries = new HashMap<>();
        entries.put("orderkey", "1");
        entries.put("orderdate", "1.1");
        entries.put("shippriority", "A");
        entries.put("revenue", "5.2");
        tuples.add(new Tuple("result", "1", entries));

        entries = new HashMap<>();
        entries.put("orderkey", "2");
        entries.put("orderdate", "1.1");
        entries.put("shippriority", "A");
        entries.put("revenue", "6.1");
        tuples.add(new Tuple("result", "2", entries));

        entries = new HashMap<>();
        entries.put("orderkey", "2");
        entries.put("orderdate", "1.1");
        entries.put("shippriority", "A");
        entries.put("revenue", "6.2");
        tuples.add(new Tuple("result", "2", entries));

        groupByAndSumRevenue(tuples).forEach(tuple -> System.out.println(tuple.toString()));
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
}
