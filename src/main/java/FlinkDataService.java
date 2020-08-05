import com.google.common.collect.ImmutableList;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import src.main.java.Relation;
import src.main.java.SchemaBuilder;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class FlinkDataService {
    private final List<String> filePaths;
    private final List<Relation> relations;
    private static String workingDirectory;

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new RuntimeException("Exactly one command line argument is expected, got " + args.length);
        }
        workingDirectory = args[0];
        requireNonNull(workingDirectory);
        File workingDir = new File(workingDirectory);
        if (!workingDir.exists() || !workingDir.isDirectory()) {
            throw new RuntimeException("Invalid working directory provided. Working directory must exit and must be a directory");
        }
        System.out.println("Using working directory as: " + workingDirectory);

        if (!workingDirectory.endsWith(File.separator)) {
            workingDirectory += File.separator;
        }


        new FlinkDataService(workingDirectory).execute();
    }

    public FlinkDataService(String workingDirectory) {
        this.filePaths = ImmutableList.copyOf(getFilePaths(workingDirectory));
        this.relations = ImmutableList.copyOf(SchemaBuilder.query3Schema());
    }

    public void execute() throws Exception {
        removeResultFile();
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        environment.addSource(new TPCHQuery3Source(filePaths, relations))
                .filter(new TPCHQuery3Filter())
                .process(new TPCHQuery3Process(relations)).addSink(new FileSink(workingDirectory));
        environment.execute("FlinkDataService");
    }

    private List<String> getFilePaths(final String workingDirectory) {
        List<String> files = new ArrayList<>();
        String filePath = workingDirectory + "lineitem.csv";
        ensureFileExists(filePath, "lineitem.csv");
        files.add(filePath);
        filePath = workingDirectory + "orders.csv";
        ensureFileExists(filePath, "orders.csv");
        files.add(filePath);
        filePath = workingDirectory + "customer.csv";
        ensureFileExists(filePath, "customer.csv");
        files.add(filePath);

        return files;
    }

    private void ensureFileExists(final String filePath, final String name) {
        if (!new File(filePath).exists()) {
            throw new RuntimeException(name + " file doesn't exist");
        }
    }

    private static void removeResultFile() {
        File resultFile = new File(workingDirectory + "result");
        if (resultFile.exists()) {
            System.out.println("WARNING deleting pre-existing result file.");
            if (!resultFile.delete()) {
                System.out.println("ERROR while deleting pre-existing result file. Exiting.");
                System.exit(1);
            }
        }
    }

}
