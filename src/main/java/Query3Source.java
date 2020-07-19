import com.google.common.collect.ImmutableList;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import src.main.java.Relation;
import src.main.java.Tuple;

import javax.annotation.Nullable;
import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Query3Source extends RichSourceFunction<Tuple> implements QuerySource {
    private volatile boolean run = false;
    private SourceContext<Tuple> context;
    private final List<String> filePaths;
    private transient List<BufferedReader> readers;
    private final List<Relation> relations;
    private final char DELIM = ',';
    private final Date CUTOFF_DATE;
    private final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");

    public Query3Source(final List<String> filePaths, final List<Relation> relations) {
        super();
        if (filePaths == null || filePaths.isEmpty()) {
            throw new RuntimeException("filePaths cannot be null or empty");
        }
        this.filePaths = ImmutableList.copyOf(filePaths);
        this.relations = relations;
        try {
            this.CUTOFF_DATE = dateFormatter.parse("1995-03-15");
        } catch (ParseException e) {
            throw new RuntimeException("Unable to parse date ", e);
        }
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
                    Tuple tuple = lineToTuple(line, counter.get() % filePaths.size());
                    if (tuple != null && isValidTuple(tuple)) {
                        System.out.println("SOURCE->" + tuple.toString());
                        sourceContext.collect(tuple);
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
                readers.add(new BufferedReader(new FileReader(new File(path))));
            } catch (FileNotFoundException e) {
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

    @Override
    public boolean isValidTuple(Tuple tuple) {
        boolean dateIsValid = true;
        if (tuple.getRelationName().equals("orders")) {
            dateIsValid = isValidDate(tuple, "orderdate", DateOperator.LESS_THAN);
        }
        if (tuple.getRelationName().equals("lineitem")) {
            dateIsValid = isValidDate(tuple, "shipdate", DateOperator.GREATER_THAN);
        }

        boolean validMarketSegment = true;
        if (tuple.getRelationName().equals("customer")) {
            validMarketSegment = isValidMarketSegment(tuple);
        }
        return dateIsValid && validMarketSegment;
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

    private boolean isValidDate(Tuple tuple, String dateColumn, DateOperator dateOperator) {
        try {
            Date date = dateFormatter.parse(tuple.getEntries().get(dateColumn).getValue());
            return dateOperator.apply(date, CUTOFF_DATE);
        } catch (ParseException e) {
            throw new RuntimeException("Unable to parse tuple date ", e);
        }
    }

    private boolean isValidMarketSegment(Tuple customerTuple) {
        return customerTuple.getEntries().get("mktsegment").getValue().equals("BUILDING");
    }

    private enum DateOperator {
        GREATER_THAN(">") {
            @Override
            public boolean apply(Date d1, Date d2) {
                return d1.after(d2);
            }
        },
        LESS_THAN("<") {
            @Override
            public boolean apply(Date d1, Date d2) {
                return d1.before(d2);
            }
        };

        private final String text;

        private DateOperator(String text) {
            this.text = text;
        }

        public abstract boolean apply(Date d1, Date d2);

        @Override
        public String toString() {
            return text;
        }
    }
}
