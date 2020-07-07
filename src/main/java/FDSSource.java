import com.google.common.collect.ImmutableList;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import src.main.java.Tuple;

import javax.annotation.Nullable;
import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FDSSource extends RichSourceFunction<String> {
    private volatile boolean run = false;
    private SourceContext<String> context;
    private final List<String> filePaths;
    private transient List<BufferedReader> readers;

    public FDSSource(final List<String> filePaths) {
        super();
        if (filePaths == null || filePaths.isEmpty()) {
            throw new RuntimeException("filePaths cannot be null or empty");
        }
        this.filePaths = ImmutableList.copyOf(filePaths);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        run = true;
    }

    @Override
    public void run(SourceContext<String> sourceContext) {
        this.context = sourceContext;
        readers = makeReaders();
        Set<BufferedReader> closedReaders = new HashSet<>();
        while (run) {
            readers.forEach(reader -> {
                try {
                    String line = reader.readLine();
                    //TODO: this tuple should be added to Tuple3 object, that object should be be added to sourceContext
                    // lineToTuple will return null if line is null, add it to Tuple3 regardless.
                    Tuple tuple = lineToTuple(line);
                    if (line != null) {
                        sourceContext.collect(line);
                    } else {
                        closedReaders.add(reader);
                        if (closedReaders.size() == filePaths.size()) {
                            cancel();
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    @Override
    public void cancel() {
        run = false;
        closeReaders();
    }

    @Nullable
    private Tuple lineToTuple(final String line) {
        //TODO: In this method: Clean the input and convert to tuple. Return null if line is null
        return null;
    }

    private List<BufferedReader> makeReaders() {
        final List<BufferedReader> readers = new ArrayList<>();
        filePaths.forEach(path -> {
            try {
                readers.add(new BufferedReader(new FileReader(new File(path))));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        });
        return readers;
    }

    private void closeReaders() {
        readers.forEach(reader -> {
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }
}
