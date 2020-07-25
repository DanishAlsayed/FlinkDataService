import org.junit.jupiter.api.Test;
import src.main.java.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class FlinkDataServiceTest {

    @Test
    void frontToBackTest() throws Exception {
        FlinkDataService fds = new FlinkDataService(asList("/home/danishalsayed/Desktop/FlinkProject/FlinkDataService/src/test/resources/lineitem_test",
                "/home/danishalsayed/Desktop/FlinkProject/FlinkDataService/src/test/resources/orders_test",
                "/home/danishalsayed/Desktop/FlinkProject/FlinkDataService/src/test/resources/customer_test"
        ));
        fds.fds();
    }

    @Test
    void testGroupByAndSumRevenue() {
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

        List<Tuple> result = TPCHQuery3Process.groupByAndSumRevenue(tuples);
        assertEquals(2,result.size());
        assertEquals("10.3",result.get(0).getEntries().get("revenue").getValue());
        assertEquals("12.3",result.get(1).getEntries().get("revenue").getValue());
    }
}
