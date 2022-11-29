package PPRL;

import java.io.Serializable;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * Class for hashing and storing string values into a bloom filter.
 * Uses double hashing, enhanced double hashing, triple hashing or random hashing, depending on hashingMode.
 * Uses bigrams.
 */
public class BloomFilter implements Serializable {

    boolean[] hashArea;
    int k; // # of hash functions to be simulated
    HashingMode mode;
    String tokenSalting;
    String h1;
    String h2;

    /**
     * Constructor for BloomFilter instance. Hash area is initialized with all 0's.
     * @param hashAreaSize The length of the hash area.
     * @param k The number of hash functions to be simulated through double hashing.
     */
    public BloomFilter(int hashAreaSize, int k, HashingMode mode, String tokenSalting, String h1, String h2) {
        hashArea = new boolean[hashAreaSize];
        Arrays.fill(hashArea, false);
        this.k = k;
        this.mode = mode;
        this.tokenSalting = tokenSalting;
        this.h1 = h1;
        this.h2 = h2;
    }

    public BloomFilter() {
    }

    /**
     * Stores each attribute, if specified with weighted attribute values.
     * @param person Person data to be stored
     */
    public void storePersonData(Person person, boolean weightedAttributes) {
        for (String attrName : Person.attributeWeights.keySet()) {
            double weight = Person.attributeWeights.get(attrName);
            if (weight == 0.0) continue; // w = 0 means identifying attribute, like IDs
            String attrVal = person.getAttributeValue(attrName);
            int k = weightedAttributes ? (int) (this.k * weight) : this.k;
            try {
                store(attrVal, k);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Splits given string into bigrams and stores all hash values of each bigram into the hash area, by simulating k
     * hash functions through double hashing.
     * @param attrValue attribute value as string.
     */
    public void store(String attrValue, int k) throws NoSuchAlgorithmException {
        List<String> bigrams = getBigrams(attrValue);
        for (String bigram : bigrams) {
            storeBigram(bigram, k);
        }
    }

    /**
     * Computes the Jaccard-Similarity (|intersection| / |union|) between this and another given Bloom filter.
     * They both must have the same hash area size.
     * @param other The Bloom Filter to compare this to.
     * @return the similarity coefficient
     */
    public double computeJaccardSimilarity(BloomFilter other) {
        if (other.getHashArea().length != hashArea.length) {
            throw new IllegalArgumentException("Bloom filters must have same hash area size.");
        }
        int union = 0;
        int intersect = 0;
        for (int i = 0; i < hashArea.length; i++) {
            if (hashArea[i] && other.getHashArea()[i]) {
                intersect++;
            } if (hashArea[i] || other.getHashArea()[i]) {
                union++;
            }
        }
        return 1.0 * intersect / union;
    }

    /**
     * Computes the Dice-Similarity (2 * |intersection| / (|X|+|Y|)) between this and another given Bloom filter.
     * They both must have the same hash area size.
     * @param other The Bloom Filter to compare this to.
     * @return the similarity coefficient
     */
    public double computeDiceSimilarity(BloomFilter other) {
        if (other.getHashArea().length != hashArea.length) {
            throw new IllegalArgumentException("Bloom filters must have same hash area size.");
        }
        int unionWithDuplicates = 0; // |X| + |Y|
        int intersect = 0;
        for (int i = 0; i < hashArea.length; i++) {
            if (hashArea[i] && other.getHashArea()[i]) {
                intersect++;
                unionWithDuplicates++;
            } if (hashArea[i] || other.getHashArea()[i]) {
                unionWithDuplicates++;
            }
        }
        return 2.0 * intersect / unionWithDuplicates;
    }

    public boolean[] getHashArea() {
        return hashArea;
    }

    /**
     * Generates from a given string the bigrams and returns them.
     * @param attrValue attribute value as string.
     * @return ArrayList of bigrams as strings.
     */
    private List<String> getBigrams(String attrValue) {
        List<String> bigrams = new ArrayList<>();
        String paddedAttrValue = "_" + attrValue + "_";
        for (int i = 0; i < paddedAttrValue.length() - 1; i++) {
            bigrams.add(paddedAttrValue.substring(i, i + 2));
        }
        return bigrams;
    }

    /**
     * Simulates k hash functions using specified hashing hashingMode based on SHA-1, MD5 and MD2. Then stores given bigram in
     * bloom filter using the simulated hash functions.
     * @param bigram bigram to be stored
     * @param k number of hash functions to be simulated
     */
    private void storeBigram(String bigram, int k) throws NoSuchAlgorithmException {
        bigram = tokenSalting + bigram + tokenSalting;
        switch (mode) {
            case DOUBLE_HASHING -> storeBigramDouble(bigram, k);
            case ENHANCED_DOUBLE_HASHING -> storeBigramEnhancedDouble(bigram, k);
            case TRIPLE_HASHING -> storeBigramTriple(bigram, k);
            case RANDOM_HASHING -> storeBigramRandom(bigram, k);
        }
    }

    private void storeBigramRandom(String bigram, int k) {
        long seed = Long.parseLong(this.tokenSalting);
        for (int i = 0; i < bigram.length(); i++) {
            seed += bigram.charAt(i) * (Math.pow(257L,  i));
        }
        Random generator = new Random(seed);
        int i = 0;
        while (i < k) {
            int hashValue = (int) (generator.nextDouble() * hashArea.length);
            hashArea[hashValue] = true;
            i++;
        }
    }

    /**
     * h_i(x) = (h1(x) + i * h2(x) + i^2 * h3(x)) mod l
     */
    private void storeBigramTriple(String bigram, int k) throws NoSuchAlgorithmException {
        BigInteger h1 = getHash(bigram, this.h1);
        BigInteger h2 = getHash(bigram, this.h2);
        BigInteger h3 = getHash(bigram, "MD2");
        int i = 0;
        while (i < k) {
            int o = Math.max(2*i - 1, 0); // i-th odd integer: 0, 1, 3, 5, 7, 9, ...
            int hashValue = h1.mod(BigInteger.valueOf(hashArea.length)).intValue();
            hashArea[hashValue] = true;
            h1 = h1.add(h2)
                    .add(h3.multiply(BigInteger.valueOf(o)));
            i++;
        }
    }

    private void storeBigramEnhancedDouble(String bigram, int k) throws NoSuchAlgorithmException {
        BigInteger h1 = getHash(bigram, this.h1);
        BigInteger h2 = getHash(bigram, this.h2);
        int i = 0;
        while (i < k) {
            int hashValue = h1.mod(BigInteger.valueOf(hashArea.length)).intValue();
            hashArea[hashValue] = true;
            h1 = h1.add(h2);
            h2 = h2.add(BigInteger.valueOf(i));
            i++;
        }
    }

    /**
     * h_i(x) = (h1(x) + i * h2(x)) mod l
     */
    private void storeBigramDouble(String bigram, int k) throws NoSuchAlgorithmException {
        BigInteger h1 = getHash(bigram, this.h1);
        BigInteger h2 = getHash(bigram, this.h2);
        int i = 0;
        while (i < k) {
            int hashValue = h1.mod(BigInteger.valueOf(hashArea.length)).intValue();
            hashArea[hashValue] = true;
            h1 = h1.add(h2);
            i++;
        }
    }

    /**
     * Function for generating a hash value from a given value using a specified hash-algorithm.
     * @param value the string to be hashed.
     * @param algorithm a string representing the hash algorithm to be used, eg. "MD5".
     * @return resulting hash value as BigInteger.
     * @throws NoSuchAlgorithmException If the specified hash-algorithm is unknown.
     */
    private BigInteger getHash(String value, String algorithm) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance(algorithm);
        digest.reset();
        digest.update(value.getBytes(StandardCharsets.UTF_8));
        return new BigInteger(1, digest.digest());
    }
}
