package PPRL;

/**
 * Class for representing a destination record (Person) in a match and the belonging similarity value.
 */
public class Match {

    private Person person;
    private double similarity;

    public Match(Person person, double similarity) {
        this.person = person;
        this.similarity = similarity;
    }

    public Person getPerson() {
        return person;
    }

    public void setPerson(Person person) {
        this.person = person;
    }

    public double getSimilarity() {
        return similarity;
    }

    public void setSimilarity(double similarity) {
        this.similarity = similarity;
    }
}
