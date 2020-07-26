import com.google.common.collect.ImmutableList;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import src.main.java.Relation;
import src.main.java.SchemaBuilder;
import src.main.java.Tuple;

import java.util.List;

import static java.util.Arrays.asList;

public class FlinkDataService {
    private final List<String> filePaths;
    private final List<Relation> relations;

    public static void main(String[] args) throws Exception {
        new FlinkDataService(asList("/home/danishalsayed/Desktop/FlinkProject/FlinkDataService/src/AcyclicJoinUnderUpdates/src/main/resources/data/lineitem_trimmed.csv",
                "/home/danishalsayed/Desktop/FlinkProject/FlinkDataService/src/AcyclicJoinUnderUpdates/src/main/resources/data/orders_trimmed.csv",
                "/home/danishalsayed/Desktop/FlinkProject/FlinkDataService/src/AcyclicJoinUnderUpdates/src/main/resources/data/customer_trimmed.csv"
        )).execute();
    }

    public FlinkDataService(final List<String> filePaths) {
        this.filePaths = ImmutableList.copyOf(filePaths);
        this.relations = ImmutableList.copyOf(SchemaBuilder.query3Schema());
    }

    public void execute() throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironment(1);
        DataStream<Tuple> stream = environment.addSource(new TPCHQuery3Source(filePaths, relations));
        stream.process(new TPCHQuery3Process(relations)).addSink(new FDSSink());
        environment.execute("FlinkDataService");
    }

}
