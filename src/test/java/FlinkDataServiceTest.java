import org.junit.jupiter.api.Test;

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
}
