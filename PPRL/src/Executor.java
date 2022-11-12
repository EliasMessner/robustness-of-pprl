package PPRL.src;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static java.util.Map.entry;

/**
 * Creates all the Bloom Filters, creates the blocking map, invokes the linking process.
 */
public class Executor {

    Parameters parameters;
    Person[] dataSet;
    ProgressHandler progressHandler;
    boolean parallel;
    Map<Person, BloomFilter> personBloomFilterMap;
    Map<String, Set<Person>> blockingMap;

    public Executor(Person[] dataSet, Parameters parameters, boolean blockingCheat, boolean parallel) {
        this.parameters = parameters;
        this.dataSet = dataSet;
        this.progressHandler = new ProgressHandler(dataSet.length, 1);
        this.parallel = parallel;
        setPersonAttributeWeights();
        // create all the bloom filters
        this.personBloomFilterMap = getPersonBloomFilterMap();
        // create the blockingKeyEncoders to generate the blockingMap
        List<BlockingKeyEncoder> blockingKeyEncoders = new ArrayList<>();
        blockingKeyEncoders.add(person -> person.getSoundex("firstName").concat(person.getAttributeValue("yearOfBirth")));
        blockingKeyEncoders.add(person -> person.getSoundex("lastName").concat(person.getAttributeValue("yearOfBirth")));
        blockingKeyEncoders.add(person -> person.getSoundex("firstName").concat(person.getSoundex("lastName")));
        // If blockingCheat turned on, use globalID as additional blocking key to avoid false negatives due to blocking
        if (blockingCheat) blockingKeyEncoders.add(person -> person.getAttributeValue("globalID"));
        // create blockingMap
        this.blockingMap = getBlockingMap(parameters.blocking(), blockingKeyEncoders.toArray(BlockingKeyEncoder[]::new));
    }

    public Set<PersonPair> getLinking() {
        Linker linker = new Linker(dataSet, progressHandler, parameters, personBloomFilterMap, blockingMap, "A", "B", parallel);
        return linker.getLinking();
    }

    private static void setPersonAttributeWeights() {
        Person.setAttributeNamesAndWeights(
                entry("sourceID", 0.0),
                entry("globalID", 0.0),
                entry("localID", 0.0),
                entry("firstName", 2.0),
                entry("middleName", 0.5),
                entry("lastName", 1.5),
                entry("yearOfBirth", 2.5),
                entry("placeOfBirth", 0.5),
                entry("country", .5),
                entry("city", .5),
                entry("zip", .3),
                entry("street", .3),
                entry("gender", 1.0),
                entry("ethnic", 1.0),
                entry("race", 1.0)
        );
    }

    /**
     * Creates a BloomFilter for each Person object in given dataset and returns a map with Person as keys and
     * BloomFilter as values.
     */
    private Map<Person, BloomFilter> getPersonBloomFilterMap() {
        progressHandler.reset();
        System.out.println("Creating Bloom Filters...");
        Map<Person, BloomFilter> personBloomFilterMap = new ConcurrentHashMap<>();
        Arrays.stream(dataSet).parallel().forEach(person -> {
            BloomFilter bf = new BloomFilter(parameters.l(), parameters.k(), parameters.hashingMode(), parameters.tokenSalting(), parameters.h1(), parameters.h2());
            bf.storePersonData(person, parameters.weightedAttributes());
            personBloomFilterMap.put(person, bf);
            progressHandler.updateProgress();
        });
        progressHandler.finish();
        return personBloomFilterMap;
    }

    /**
     * Assigns each entry in given dataset to a blocking key and returns the resulting map. If blocking is turned off, maps
     * all records to the same blocking key "DUMMY_VALUE".
     *
     * @param blocking true if blocking is turned on, else false
     * @return a map that maps each blocking key to a set of records encoded by that key.
     */
    private Map<String, Set<Person>> getBlockingMap(Boolean blocking, BlockingKeyEncoder... blockingKeyEncoders) {
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
