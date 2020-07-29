import src.main.java.Tuple;

import java.io.BufferedReader;
import java.util.List;

public interface QuerySource {
    Tuple lineToTuple(final String line, int index);
    List<BufferedReader> makeReaders();
    void closeReaders();

}
