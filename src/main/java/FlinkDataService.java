import com.google.common.collect.ImmutableList;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import src.main.java.Relation;
import src.main.java.SchemaBuilder;
import src.main.java.Tuple;

import java.util.List;

public class FlinkDataService {
    private final List<String> filePaths;
    private final List<Relation> relations;

    public FlinkDataService(final List<String> filePaths) {
        this.filePaths = ImmutableList.copyOf(filePaths);
        this.relations = ImmutableList.copyOf(SchemaBuilder.query3Schema());
    }

    public void readFile() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Tuple> stream = env.addSource(new FDSSource(filePaths, relations));
        stream.print();
        env.execute("FlinkDataService");
    }

}
