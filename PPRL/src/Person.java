package PPRL.src;

import org.apache.commons.codec.language.Soundex;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Class representing a person, with the attributes as in the dataset.
 */
public class Person {

    public static String[] attributeNames;
    public String[] attributeValues;
    public static LinkedHashMap<String, Double> attributeWeights;

    public Person(String... attributeValues) {
        if (attributeValues.length != attributeNames.length) {
            throw new IllegalArgumentException("Attribute array must have " + attributeNames.length + " elements.");
        }
        this.attributeValues = attributeValues;
    }

    /**
     * Will statically store the attribute names (keys) and weights (key, value)-pairs into the class.
     * The input order of the entries will be represented in the map, and is important for the constructor.
     * Hence, this method must be called before first constructor call.
     * Weight = 0.0 means the attribute will never be stored in a BloomFilter because it is an identifying attribute,
     * like globalID etc.
     * @param entries (attributeName, weight) - pairs in the order they should be stored in the map.
     */
    @SafeVarargs
    public static void setAttributeNamesAndWeights(Map.Entry<String, Double>... entries) {
        attributeWeights = new LinkedHashMap<>();
        for (Map.Entry<String, Double> entry : entries) {
            attributeWeights.put(entry.getKey(), entry.getValue());
        }
        Person.attributeNames = attributeWeights.keySet().toArray(new String[0]);
    }

    /**
     * Returns the value of the attribute specified by its column name.
     * @param key the column name
     * @return the attribute value as string.
     * @throws IllegalArgumentException if the specified attribute name does not exist.
     */
    public String getAttributeValue(String key) {
        for (int index = 0; index < attributeNames.length; index++) {
            if (attributeNames[index].equals(key)) return attributeValues[index];
        }
        throw new IllegalArgumentException("No such attribute '" + key + "'");
    }

    public boolean equalGlobalID(Person other) {
        return this.attributeValues[1].equals(other.attributeValues[1]);
    }

    public String getSoundex(String attributeName) {
        Soundex soundex = new Soundex();
        return soundex.soundex(this.getAttributeValue(attributeName));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Person person = (Person) o;
        return Arrays.equals(attributeValues, person.attributeValues);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(attributeValues);
    }
}
