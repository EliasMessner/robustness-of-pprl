package RecordLinkageInterface.src;

import PPRL.src.Executor;
import PPRL.src.Parameters;
import PPRL.src.Person;
import PPRL.src.PersonPair;

import java.io.IOException;
import java.util.Set;

public class PPRLAdapter implements RecordLinkageI {

    /**
     * parameters = l, k, t in that order
     */
    @Override
    public void getLinking(String fromFile, String outFile, String... params) {
        Person[] dataSet = getDatasetFromFile(fromFile);
        Parameters parameters = getParametersObject();
        boolean blockingCheat = true;
        boolean parallel = true;
        Executor executor = new Executor(dataSet, parameters, blockingCheat, parallel);
        Set<PersonPair> linking = executor.getLinking();
        storeLinkingToFile(linking, outFile);
    }

    private Person[] getDatasetFromFile(String fromFile) {
        throw new UnsupportedOperationException();  // TODO
    }

    private Parameters getParametersObject() {
        throw new UnsupportedOperationException();  // TODO
    }

    private void storeLinkingToFile(Set<PersonPair> linking, String outFile) {
        throw new UnsupportedOperationException();  // TODO
    }
}
