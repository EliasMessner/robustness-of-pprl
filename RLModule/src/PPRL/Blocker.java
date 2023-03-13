package PPRL;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static java.util.Map.entry;

public class Blocker {

    boolean blockingCheat, blocking, parallel;
    ProgressHandler progressHandler;

    public Blocker(boolean blocking, boolean blockingCheat, boolean parallel) {
        this.blocking = blocking;
        this.blockingCheat = blockingCheat;
        this.parallel = parallel;
    }

    /**
     * Creates blockingKeyEncoders and assigns each entry in given dataset to a blocking key and returns the resulting map. If blocking is turned off, maps
     * all records to the same blocking key "DUMMY_VALUE".
     *
     * @return a map that maps each blocking key to a set of records encoded by that key.
     */
    public Map<String, Set<Person>> getBlockingMap(Person[] dataSet) {
        this.progressHandler = new ProgressHandler(dataSet.length, 1);
        return getBlockingMap(dataSet, getBlockingKeyEncoders());
    }

    /**
     * Assigns each entry in given dataset to a blocking key and returns the resulting map. If blocking is turned off, maps
     * all records to the same blocking key "DUMMY_VALUE".
     *
     * @return a map that maps each blocking key to a set of records encoded by that key.
     */
    public Map<String, Set<Person>> getBlockingMap(Person[] dataSet, BlockingKeyEncoder... blockingKeyEncoders) {
        Map<String, Set<Person>> blockingMap;
        if (!blocking) {
            return Map.ofEntries(entry("DUMMY_VALUE", new HashSet<>(Arrays.asList(dataSet))));
        }
        System.out.println("Creating Blocking Keys...");
        progressHandler.reset();
        progressHandler.setTotalSize(dataSet.length);
        blockingMap = mapRecordsToBlockingKeys(dataSet, progressHandler, blockingKeyEncoders);
        progressHandler.finish();
        return blockingMap;
    }

    private BlockingKeyEncoder[] getBlockingKeyEncoders() {
        // create the blockingKeyEncoders to generate the blockingMap
        List<BlockingKeyEncoder> blockingKeyEncoders = new ArrayList<>();
        blockingKeyEncoders.add(person -> person.getSoundex("firstName").concat(person.getAttributeValue("yearOfBirth")));
        blockingKeyEncoders.add(person -> person.getSoundex("lastName").concat(person.getAttributeValue("yearOfBirth")));
        blockingKeyEncoders.add(person -> person.getSoundex("firstName").concat(person.getSoundex("lastName")));
        // If blockingCheat turned on, use globalID as additional blocking key to avoid false negatives due to blocking
        if (blockingCheat) blockingKeyEncoders.add(person -> person.getAttributeValue("globalID"));
        return blockingKeyEncoders.toArray(BlockingKeyEncoder[]::new);
    }

    /**
     * Helper method for getBlockingMap.
     */
    private ConcurrentHashMap<String, Set<Person>> mapRecordsToBlockingKeys(Person[] dataSet, ProgressHandler progressHandler, BlockingKeyEncoder... blockingKeyEncoders) {
        ConcurrentHashMap<String, Set<Person>> blockingMap = new ConcurrentHashMap<>();
        Stream<Person> stream = Arrays.stream(dataSet);
        if (parallel) stream = stream.parallel();
        stream.forEach(person -> {
            for (BlockingKeyEncoder blockingKeyEncoder : blockingKeyEncoders) {
                String blockingKey = blockingKeyEncoder.encode(person);
                blockingMap.putIfAbsent(blockingKey, new HashSet<>());
                blockingMap.get(blockingKey).add(person);
            }
            progressHandler.updateProgress();
        });
        return blockingMap;
    }
}
