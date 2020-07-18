import src.main.java.Relation;
import src.main.java.Tuple;

public interface QueryProcess {
    Relation calculateResult(Tuple tuple);
}
