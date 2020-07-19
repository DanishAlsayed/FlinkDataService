import com.google.common.collect.ImmutableList;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import src.main.java.Relation;
import src.main.java.SchemaBuilder;
import src.main.java.Tuple;

import java.util.List;
import java.util.Properties;

public class FlinkDataService {
    private final List<String> filePaths;
    private final List<Relation> relations;

    public FlinkDataService(final List<String> filePaths) {
        this.filePaths = ImmutableList.copyOf(filePaths);
        this.relations = ImmutableList.copyOf(SchemaBuilder.query3Schema());
    }

    public void fds() throws Exception {
        Properties properties = new Properties();
        properties.put("jobmanager.heap.size", "4096");

        Configuration configuration = ConfigurationUtils.createConfiguration(properties);
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironment(1, configuration);
        environment.setParallelism(1);
        DataStream<Tuple> stream = environment.addSource(new TPCHQuery3Source(filePaths, relations));
        stream.process(new TPCHQuery3Process(relations)).addSink(new FDSSink());
        environment.execute("FlinkDataService");
    }

}
