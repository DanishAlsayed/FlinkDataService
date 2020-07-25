import com.google.common.collect.ImmutableList;
import org.apache.commons.io.FileUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import src.main.java.Relation;
import src.main.java.Tuple;

import javax.annotation.Nullable;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TPCHQuery3Source extends RichSourceFunction<Tuple> implements QuerySource {
    private volatile boolean run = false;
    private SourceContext<Tuple> context;
    private final List<String> filePaths;
    private transient List<BufferedReader> readers;
    private final List<Relation> relations;
    private final char DELIM = ',';
    private final Date CUTOFF_DATE;
    private final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
    private int lineitemCounter = 0;

    public TPCHQuery3Source(final List<String> filePaths, final List<Relation> relations) {
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
        /*List<String> file1Lines;
        List<String> file2Lines;
        List<String> file3Lines;
        try {
            file1Lines = FileUtils.readLines(new File(filePaths.get(0)), StandardCharsets.ISO_8859_1);
            file2Lines = FileUtils.readLines(new File(filePaths.get(1)), StandardCharsets.ISO_8859_1);
            file3Lines = FileUtils.readLines(new File(filePaths.get(2)), StandardCharsets.ISO_8859_1);
        } catch (Exception e) {
            throw new RuntimeException("Error in reading file", e);
        }
        Iterator<String> file1Iterator = file1Lines.iterator();
        Iterator<String> file2Iterator = file2Lines.iterator();
        Iterator<String> file3Iterator = file3Lines.iterator();
        List<Iterator<String>> files = new ArrayList<>();
        files.add(file1Iterator);
        files.add(file2Iterator);
        files.add(file3Iterator);*/

        while (run) {
            readers.forEach(reader -> {
                /*int index = counter.get() % filePaths.size();
                Iterator<String> iterator = files.get(index);
                if (iterator.hasNext()) {
                    String line = iterator.next();
                    Tuple tuple = lineToTuple(line, index);
                    if (tuple != null && isValidTuple(tuple)) {
                        sourceContext.collect(tuple);
                    }
                }
                counter.getAndIncrement();*/
                try {
                    String line = reader.readLine();
                    if (line != null) {
                        int index = counter.get() % filePaths.size();
                        Tuple tuple = lineToTuple(line, index);
                        //Note: ensure the isValidTuple check is done if _trimmed2.csv date files are not being used
                        if (tuple != null && isValidTuple(tuple)) {
                            //System.out.println("SOURCE->" + tuple.toString());
                            sourceContext.collect(tuple);
                        }
                    } else {
                        //readers.remove(reader);
                        closedReaders.add(reader);
                        //TODO: remove the -1, we should process all of the lineitem rows, its the biggest file so will be the last to close
                        if (closedReaders.size() == filePaths.size()) {
                            System.out.println("ALL FILES HAVE BEEN STREAMED");
                            cancel();
                        }
                    }
                    //Marking line String for garbage collection
                    line = null;
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
                System.out.println("Garbage collector called");
                System.gc();
            }
            System.out.println(lineitemCounter);
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
