package RecordLinkageInterface.src;

import java.io.IOException;

public interface RecordLinkageI {

    // public void getLinking(String fromFile, String outFile, String configFile);
    void getLinking(String fromFile, String outFile, String... parameters) throws IOException;
}
