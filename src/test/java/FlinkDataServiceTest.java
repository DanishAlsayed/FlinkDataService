import org.junit.jupiter.api.Test;

import static java.util.Arrays.asList;

public class FlinkDataServiceTest {

    @Test
    void readFileTest() throws Exception {
        FlinkDataService fds = new FlinkDataService(asList("/home/danishalsayed/Desktop/FlinkProject/FlinkDataService/src/test/resources/TestFile1",
                "/home/danishalsayed/Desktop/FlinkProject/FlinkDataService/src/test/resources/TestFile2"));
        fds.readFile();
    }
}
