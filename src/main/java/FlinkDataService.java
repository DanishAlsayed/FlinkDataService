import com.google.common.collect.ImmutableList;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import src.main.java.Relation;
import src.main.java.SchemaBuilder;

import java.io.File;
import java.util.List;

import static java.util.Arrays.asList;

public class FlinkDataService {
    private final List<String> filePaths;
    private final List<Relation> relations;

    public static void main(String[] args) throws Exception {
        new FlinkDataService(asList("lineitem.csv", "orders.csv", "customer.csv")).execute();
    }

    public FlinkDataService(final List<String> filePaths) {
        this.filePaths = ImmutableList.copyOf(filePaths);
        this.relations = ImmutableList.copyOf(SchemaBuilder.query3Schema());
    }

    public void execute() throws Exception {
        removeResultFile();
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        environment.addSource(new TPCHQuery3Source(filePaths, relations))
                .filter(new TPCHQuery3Filter())
                .process(new TPCHQuery3Process(relations)).addSink(new FileSink());
        environment.execute("FlinkDataService");
    }

    private static void removeResultFile() {
        File resultFile = new File("result");
        if (resultFile.exists()) {
            System.out.println("WARNING deleting pre-existing result file.");
            if (!resultFile.delete()) {
                System.out.println("ERROR while deleting pre-existing result file. Exiting.");
                System.exit(1);
            }
        }
    }

}
