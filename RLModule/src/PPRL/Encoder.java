package PPRL;

import java.io.*;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Encoder {

    String pbmPath;
    Person[] dataSet;
    EncoderParams parameters;
    ProgressHandler progressHandler;
    Map<Person, BloomFilter> personBloomFilterMap;

    public Encoder(Person[] dataSet, EncoderParams parameters, String personBloomFilterMapPath) {
        this.pbmPath = personBloomFilterMapPath;
        this.dataSet = dataSet;
        this.parameters = parameters;
        this.progressHandler = new ProgressHandler(dataSet.length, 1);
    }

    public Map<Person, BloomFilter> getPersonBloomFilterMap() {
        return personBloomFilterMap;
    }

    /**
     * Check if personBloomFilterMap is stored in the specified filepath. If yes, load it,
     * otherwise, create a new one and write it.
     * After calling this method the field personBloomFilterMap will be set.
     */
    public void createPbmIfNotExist() {
        if (!pbmExists()) {
            this.personBloomFilterMap = createPersonBloomFilterMap();
            System.out.println("Saving Bloom Filters...");
            savePbm();
            System.out.println("Done.");
        } else {
            System.out.println("Found Existing Bloom Filters. Loading...");
            loadPbm();
            System.out.println("Done.");
        }
    }

    /**
     * Creates a BloomFilter for each Person object in given dataset and returns a map with Person as keys and
     * BloomFilter as values.
     */
    private Map<Person, BloomFilter> createPersonBloomFilterMap() {
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

    private void loadPbm() {
        File file = new File(pbmPath);
        try (FileInputStream fis = new FileInputStream(file);
             ObjectInputStream ois = new ObjectInputStream(fis)) {
            this.personBloomFilterMap = (ConcurrentHashMap<Person, BloomFilter>) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private void savePbm() {
        File file = new File(pbmPath);
        try (FileOutputStream fos = new FileOutputStream(file);
             ObjectOutputStream oos = new ObjectOutputStream(fos)) {
            oos.writeObject(this.personBloomFilterMap);
            oos.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean pbmExists() {
        return (new File(pbmPath).isFile());
    }

}
