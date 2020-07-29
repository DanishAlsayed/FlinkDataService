import org.apache.flink.api.common.functions.RichFilterFunction;
import src.main.java.Tuple;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TPCHQuery3Filter extends RichFilterFunction<Tuple> implements QueryFilter {
    private final Date CUTOFF_DATE;
    private final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");

    public TPCHQuery3Filter() {
        try {
            this.CUTOFF_DATE = dateFormatter.parse("1995-03-15");
        } catch (ParseException e) {
            throw new RuntimeException("Unable to parse date ", e);
        }
    }

    @Override
    public boolean filter(Tuple tuple) {
        return isValidTuple(tuple);
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
