package RecordLinkageInterface.src;

import PPRL.src.*;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class PPRLAdapter implements RecordLinkageI {
    
    Launcher launcher;
    
    public PPRLAdapter() {
        boolean blockingCheat = true;
        boolean parallel = true;
        this.launcher = new Launcher(blockingCheat, parallel);
    }

    public PPRLAdapter(boolean blockingCheat, boolean parallel) {
        this.launcher = new Launcher(blockingCheat, parallel);
    }

    /**
     * Reads dataset from file and prepares the launcher.
     * parameters = l, k, t in that order TODO config file
     */
    public void readData(String fromFile, String... params) {
        Person[] dataSet = getDatasetFromFile(fromFile);
        Parameters parameters = getParametersObject(params);
        launcher.prepare(dataSet, parameters);
    }

    /**
     * Invokes linkage process and stores result to file.
     */
    @Override
    public void getLinking(String outFile) {
        Set<PersonPair> linking = launcher.getLinking();
        storeLinkingToFile(linking, outFile);
    }

    private Person[] getDatasetFromFile(String filePath) {
        List<Person> records = new ArrayList<>();
        try (CSVReader csvReader = new CSVReader(new FileReader(filePath))) {
            String[] values;
            while ((values = csvReader.readNext()) != null) {
                records.add(new Person(values));
            }
        } catch (IOException | CsvValidationException e) {
            throw new RuntimeException(e.getMessage());
        }
        return records.toArray(Person[]::new);
    }

    private Parameters getParametersObject(String... params) {
        int l = Integer.parseInt(params[0]);
        int k = Integer.parseInt(params[1]);
        double t = Double.parseDouble(params[2]);
        return new Parameters(
                LinkingMode.POLYGAMOUS,
                HashingMode.ENHANCED_DOUBLE_HASHING,
                "SHA-1",
                "MD5",
                true,
                true,
                "",
                l, k, t);
    }

    private void storeLinkingToFile(Set<PersonPair> linking, String outFilePath) {
        File file = new File(outFilePath);
        try (CSVWriter writer = new CSVWriter(new FileWriter(file))) {
            String[] header = {"globalID_A", "globalID_B"};
            writer.writeNext(header);
            for (PersonPair pair : linking) {
                String[] nextLine = {
                        pair.getA().getAttributeValue("globalID"),
                        pair.getB().getAttributeValue("globalID")
                };
                writer.writeNext(nextLine);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
