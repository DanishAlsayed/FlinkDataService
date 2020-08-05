import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import src.main.java.Tuple;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class FileSink extends RichSinkFunction<List<Tuple>> {
    private transient BufferedWriter writer;
    private final String workingDirectory;

    FileSink(final String workingDirectory) {
        this.workingDirectory = workingDirectory;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        String resultPath = workingDirectory + "result";
        File resultFile = new File(resultPath);
        assert !resultFile.exists() || resultFile.delete();
        writer = new BufferedWriter(new FileWriter(resultPath, true));
    }

    @Override
    public void invoke(List<Tuple> result, Context context) throws IOException {
        if (result.size() == 0)
            return;

        result.forEach(tuple -> {
            try {
                writer.write(tuple.toString() + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void close() throws Exception {
        super.close();
        writer.close();
    }
}
