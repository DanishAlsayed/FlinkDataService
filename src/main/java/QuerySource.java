import src.main.java.Tuple;

import java.io.BufferedReader;
import java.util.List;

public interface QuerySource {
    boolean isValidTuple(Tuple tuple);
    Tuple lineToTuple(final String line, int index);
    List<BufferedReader> makeReaders();
    void closeReaders();

}
