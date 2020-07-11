import com.google.common.collect.ImmutableList;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import src.main.java.Relation;
import src.main.java.Tuple;

import javax.annotation.Nullable;
import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class FDSSource extends RichSourceFunction<Tuple> {
    private volatile boolean run = false;
    private SourceContext<Tuple> context;
    private final List<String> filePaths;
    private transient List<BufferedReader> readers;
    private final List<Relation> relations;
    private final char DELIM = '\t';

    public FDSSource(final List<String> filePaths, final List<Relation> relations) {
        super();
        if (filePaths == null || filePaths.isEmpty()) {
            throw new RuntimeException("filePaths cannot be null or empty");
        }
        this.filePaths = ImmutableList.copyOf(filePaths);
        this.relations = relations;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        run = true;
    }

    @Override
    public void run(SourceContext<Tuple> sourceContext) {
        this.context = sourceContext;
        readers = makeReaders();
        Set<BufferedReader> closedReaders = new HashSet<>();
        AtomicInteger counter = new AtomicInteger();
        while (run) {
            readers.forEach(reader -> {
                try {
                    String line = reader.readLine();
                    Tuple tuple = lineToTuple(line, counter.get() % filePaths.size());
                    System.out.println(counter);
                    if (line != null) {
                        sourceContext.collect(tuple);
                    } else {
                        closedReaders.add(reader);
                        if (closedReaders.size() == filePaths.size()) {
                            cancel();
                        }
                    }
                    counter.getAndIncrement();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    @Override
    public void cancel() {
        run = false;
    }

    @Nullable
    private Tuple lineToTuple(final String line, int index) {
        if (line == null) {
            return null;
        }
        Relation relation = relations.get(index);
        String[] values = line.split(String.valueOf(DELIM));
        Map<String, String> entries = makeEntries(relation, values);
        return new Tuple(relation.getName(), getPrimaryKeyValue(relation, values), entries);
    }

    private String getPrimaryKeyValue(Relation relation, String[] values) {
        int pkIndex = relation.getColumnNamesList().indexOf(relation.getPrimaryKeyName());
        return values[pkIndex];
    }

    private Map<String, String> makeEntries(Relation relation, String[] values) {
        Map<String, String> entries = new HashMap<>();

        List<String> columnNames = relation.getColumnNamesList();
        int count = values.length;
        if (columnNames.size() != count) {
            throw new RuntimeException("# of columns for relation " + relation.getName() + " must be equal to the # of values." + count + " != " + columnNames.size());
        }
        for (int i = 0; i < count; i++) {
            entries.put(columnNames.get(i), values[i]);
        }
        return entries;
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
