package PPRL;

import java.util.Map;
import java.util.Set;

import static java.util.Map.entry;

/**
 * Creates all the Bloom Filters, creates the blocking map, invokes the linking process.
 */
public class Launcher {

    Matcher matcher;
    Encoder encoder;
    Blocker blocker;
    EncoderParams encoderParams;
    MatcherParams matcherParams;
    Person[] dataSet;
    ProgressHandler progressHandler;
    boolean blockingCheat, parallelBlockingMapCreation, parallelLinking, alwaysRecreateBloomFilters;
    Map<String, Set<Person>> blockingMap;

    public Launcher(boolean blockingCheat, boolean parallelBlockingMapCreation, boolean parallelLinking, boolean alwaysRecreateBloomFilters) {
        this.blockingCheat = blockingCheat;
        this.parallelBlockingMapCreation = parallelBlockingMapCreation;
        this.parallelLinking = parallelLinking;
        this.alwaysRecreateBloomFilters = alwaysRecreateBloomFilters;
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
        this.encoderParams = encoderParams;
        this.matcherParams = matcherParams;
        prepareEncoder(encoderParams, personBloomFilterMapPath);
        prepareBlocker();
        prepareMatcher(dataSet, matcherParams);
    }

    private void prepareEncoder(EncoderParams encoderParams, String personBloomFilterMapPath) {
        this.encoder = new Encoder(this.dataSet, encoderParams, personBloomFilterMapPath);
        // create all the bloom filters, or load from file if they exist
        encoder.createPbmIfNotExist(true);
    }

    private void prepareBlocker() {
        this.blocker = new Blocker(this.matcherParams.blocking(), this.blockingCheat, this.parallelBlockingMapCreation);
    }

    private void prepareMatcher(Person[] dataSet, MatcherParams matcherParams) {
        this.blockingMap = this.blocker.getBlockingMap(this.dataSet);
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


}
