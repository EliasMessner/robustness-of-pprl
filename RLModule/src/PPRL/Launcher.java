package PPRL;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static java.util.Map.entry;

/**
 * Creates all the Bloom Filters, creates the blocking map, invokes the linking process.
 */
public class Launcher {

    Matcher matcher;
    Encoder encoder;
    EncoderParams encoderParams;
    MatcherParams matcherParams;
    Person[] dataSet;
    ProgressHandler progressHandler;
    boolean blockingCheat;
    boolean parallelBlockingMapCreation, parallelLinking;
    Map<String, Set<Person>> blockingMap;

    public Launcher(boolean blockingCheat, boolean parallelBlockingMapCreation, boolean parallelLinking) {
        this.blockingCheat = blockingCheat;
        this.parallelBlockingMapCreation = parallelBlockingMapCreation;
        this.parallelLinking = parallelLinking;
        setPersonAttributeWeights();
    }

    /**
     * Creates all Bloom filters, creates the blocking map.
     * @param dataSet the dataset used for linking
     * @param encoderParams Params used for Bloom Filter creation.
     * @param matcherParams Params used for linking.
     */
    public void prepare(Person[] dataSet, EncoderParams encoderParams, MatcherParams matcherParams, String personBloomFilterMapPath) {
        this.dataSet = dataSet;
        setPersonAttributeWeights();
        this.progressHandler = new ProgressHandler(dataSet.length, 1);
        prepareEncoder(dataSet, encoderParams, personBloomFilterMapPath);
        prepareMatcher(dataSet, matcherParams);
    }

    private void prepareEncoder(Person[] dataSet, EncoderParams encoderParams, String personBloomFilterMapPath) {
        this.encoderParams = encoderParams;
        this.encoder = new Encoder(dataSet, encoderParams, personBloomFilterMapPath);
        // create all the bloom filters, or load from file if they exist
        encoder.createPbmIfNotExist();
    }

    private void prepareMatcher(Person[] dataSet, MatcherParams matcherParams) {
        this.matcherParams = matcherParams;
        this.blockingMap = getBlockingMap(getBlockingKeyEncoders());
        this.matcher = new Matcher(dataSet, matcherParams, encoder.getPersonBloomFilterMap(), blockingMap, "A", "B", parallelLinking);
    }

    /**
     * Invokes the linkage process.
     * @return a set of all matches pairs.
     */
    public Set<PersonPair> getLinking() {
        return matcher.getLinking();
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
     * Assigns each entry in given dataset to a blocking key and returns the resulting map. If blocking is turned off, maps
     * all records to the same blocking key "DUMMY_VALUE".
     *
     * @return a map that maps each blocking key to a set of records encoded by that key.
     */
    private Map<String, Set<Person>> getBlockingMap(BlockingKeyEncoder... blockingKeyEncoders) {
        Map<String, Set<Person>> blockingMap;
        if (!matcherParams.blocking()) {
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
        if (parallelBlockingMapCreation) stream = stream.parallel();
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
