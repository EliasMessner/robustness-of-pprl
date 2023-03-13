package RLInterface;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

class MainTest {

    @Test
    public void testMain() {
        Main.main(new String[]{
                "-d", "testResources/test_data.csv",
                "-o", "testResources/test_out.csv",
                "-c", "testResources/default_config.json"
        });
        List<String> expectation = getLines("testResources/test_out_expectation.csv");
        List<String> observed = getLines("testResources/test_out.csv");
        assertTrue(equalsIgnoreOrder(expectation, observed));
    }

    public <E> boolean equalsIgnoreOrder(List<E> first, List<E> second) {
        return (first.size() == second.size()
                && first.containsAll(second)
                && second.containsAll(first));
    }

    public static List<String> getLines(String filePath) {
        List<String> result = new ArrayList<>();
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                result.add(line);
            }
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
        return result;
    }

}