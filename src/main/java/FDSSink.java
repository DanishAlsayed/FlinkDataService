import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import src.main.java.Relation;
import src.main.java.Tuple;

import java.util.List;

public class FDSSink implements SinkFunction<List<Tuple>> {

    FDSSink() {
    }

    @Override
    public void invoke(List<Tuple> result, Context context) {
        if (result.size() == 0)
            return;
        System.out.println("RESULT: " + result.size());
        result.forEach(tuple -> {
            System.out.println(tuple.getPrimaryKeyValue() + "==>" + tuple.toString());
        });
        System.out.println("__________________________________");
    }
}
