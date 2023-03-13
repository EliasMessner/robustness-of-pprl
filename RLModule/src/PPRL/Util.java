package PPRL;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class Util {

    public static <T> void checkIfNoneNull(T[] array) {
        for (T t : array) {
            if (t == null) throw new NullPointerException();
        }
    }

    public static <T> void checkIfNoneNull(Iterable<T> iterable) {
        for (T t : iterable) {
            if (t == null) throw new NullPointerException();
        }
    }

    public static <T> void forEachOptionalParallelism(Stream<T> stream, Consumer<? super T> consumer, boolean parallel) {
        if (parallel) stream = stream.parallel();
        stream.forEach(consumer);
    }

    public static Person[] getDatasetFromFile(String filePath) {
        List<Person> records = new ArrayList<>();
        try (CSVReader csvReader = new CSVReader(new FileReader(filePath))) {
            String[] values;
            while ((values = csvReader.readNext()) != null) {
                records.add(new Person(values));
            }
        } catch (IOException | CsvValidationException e) {
            throw new RuntimeException(e.getMessage());
        }
        return records.toArray(Person[]::new);
    }

}
