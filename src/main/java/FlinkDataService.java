import com.google.common.collect.ImmutableList;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

public class FlinkDataService {
    final List<String> filePaths;
    public FlinkDataService(final List<String> filePaths) {
        this.filePaths = ImmutableList.copyOf(filePaths);
    }

    public void readFile() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> stream = env.addSource(new FDSSource(filePaths));

        stream.print();

        //DataStream<String> inputStream = env.readTextFile("/home/danishalsayed/Desktop/FlinkProject/FlinkDataService/src/AcyclicJoinUnderUpdates/src/main/resources/data/customer.csv");
        //DataStream<String> inputStream2 = env.readTextFile("/home/danishalsayed/Desktop/FlinkProject/FlinkDataService/src/AcyclicJoinUnderUpdates/src/main/resources/data/orders.csv");
        //DataStream<String> combinedStream = inputStream.union(inputStream2);
        env.execute("FlinkDataService");
    }

}
