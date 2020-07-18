import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import src.main.java.Relation;

public class FDSSink implements SinkFunction<Relation> {

    FDSSink() {
    }

    @Override
    public void invoke(Relation value, Context context) throws Exception {
        if (value.getTuples().size() == 0)
            return;
        System.out.println("RESULT: ");
        value.getTuples().forEach((k, v) -> {
            System.out.println(k + "==>" + v.toString());
        });
    }
}
