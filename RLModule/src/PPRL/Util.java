package PPRL;

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

}
