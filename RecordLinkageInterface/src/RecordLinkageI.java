package RecordLinkageInterface.src;

public interface RecordLinkageI {

    void readData(String fromFile, String... params);

    void getLinking(String outFile);

}
