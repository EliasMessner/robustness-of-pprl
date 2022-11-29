package RLInterface;

public interface RecordLinkageI {

    void readData(String fromFile, String configFile, String personBloomFilterMapPath);

    void getLinking(String outFile);

}
