import com.google.common.collect.ImmutableList;
import org.apache.commons.io.FileUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import src.main.java.Relation;
import src.main.java.Tuple;

import javax.annotation.Nullable;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TPCHQuery3Source extends RichSourceFunction<Tuple> implements QuerySource {
    private volatile boolean run = false;
    private SourceContext<Tuple> context;
    private final List<String> filePaths;
    private transient List<BufferedReader> readers;
    private final List<Relation> relations;
    private final char DELIM = ',';
    private int lineitemCounter = 0;

    public TPCHQuery3Source(final List<String> filePaths, final List<Relation> relations) {
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
    public void close() throws Exception {
        closeReaders();
        super.close();
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
                    if (line != null) {
                        int index = counter.get() % filePaths.size();
                        Tuple tuple = lineToTuple(line, index);
                        if (tuple != null) {
                            sourceContext.collect(tuple);
                        }
                    } else {
                        closedReaders.add(reader);
                        if (closedReaders.size() == filePaths.size()) {
                            System.out.println("ALL FILES HAVE BEEN STREAMED");
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
    @Override
    public Tuple lineToTuple(final String line, int index) {
        if (line == null) {
            return null;
        }
        if (index == 0) {
            lineitemCounter++;
            if (lineitemCounter == 1000000 || lineitemCounter == 1500000 || lineitemCounter == 2000000 || lineitemCounter == 2500000 || lineitemCounter == 2750000) {
                System.gc();
            }
        }

        Relation relation = relations.get(index);
        String[] values = line.split(String.valueOf(DELIM));
        Map<String, String> entries = makeEntries(relation, values);
        return new Tuple(relation.getName(), getPrimaryKeyValue(relation, values), entries);
    }

    @Override
    public List<BufferedReader> makeReaders() {
        final List<BufferedReader> readers = new ArrayList<>();
        filePaths.forEach(path -> {
            try {
                readers.add(new BufferedReader(new FileReader(new File(path), StandardCharsets.ISO_8859_1)));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        return readers;
    }

    @Override
    public void closeReaders() {
        readers.forEach(reader -> {
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    private String getPrimaryKeyValue(Relation relation, String[] values) {
        if (relation.getName().equals("lineitem")) {
            int linenumberIndex = relation.getColumnNamesList().indexOf(relation.getPrimaryKeyName());
            int orderkeyIndex = relation.getColumnNamesList().indexOf("orderkey");
            StringBuilder pk = new StringBuilder();
            pk.append(values[orderkeyIndex]);
            pk.append(values[linenumberIndex]);
            return pk.toString();
        }
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
            entries.put(columnNames.get(i).intern(), values[i].intern());
        }
        return entries;
    }
}
