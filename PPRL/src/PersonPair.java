package PPRL.src;

import java.util.HashSet;
import java.util.Set;

/**
 * Undirected pair of Person objects. (A, B) equals (B, A)
 */
public class PersonPair {

    private Person A;
    private Person B;
    private Set<Person> representationAsSet;

    public PersonPair(Person a, Person b) {
        A = a;
        B = b;
        representationAsSet = new HashSet<>();
        representationAsSet.add(A);
        representationAsSet.add(B);
    }

    public Person getA() {
        return A;
    }

    public Person getB() {
        return B;
    }

    public void set(Person a, Person b) {
        A = a;
        B = b;
        representationAsSet.clear();
        representationAsSet.add(A);
        representationAsSet.add(B);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PersonPair that = (PersonPair) o;
        return representationAsSet.equals(that.representationAsSet);
    }

    @Override
    public int hashCode() {
        return representationAsSet.hashCode();
    }

    public boolean contains(Person p) {
        return representationAsSet.contains(p);
    }
}
