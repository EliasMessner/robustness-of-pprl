package RLInterface;

import PPRL.*;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

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
     * Reads dataset and config file and prepares the launcher.
     */
    public void readData(String fromFile, String configFile) {
        try {
            Person[] dataSet = getDatasetFromFile(fromFile);
            Parameters parameters = getParametersObject(configFile);
            launcher.prepare(dataSet, parameters);
        } catch (IOException | ParseException e) {
            throw new RuntimeException(e);
        }
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

    private Parameters getParametersObject(String configFile) throws IOException, ParseException {
        try (FileReader reader = new FileReader(configFile)) {
            JSONObject jsonObject = (JSONObject) (new JSONParser().parse(reader));
            int l = (int) (long) jsonObject.get("l");
            int k = (int) (long) jsonObject.get("k");
            double t = (double) jsonObject.get("t");
            String tokenSalting = ((Long) jsonObject.get("seed")).toString();
            return new Parameters(
                    LinkingMode.POLYGAMOUS,
                    HashingMode.ENHANCED_DOUBLE_HASHING,
                    "SHA-1",
                    "MD5",
                    true,
                    true,
                    tokenSalting,
                    l, k, t);
        }
    }

    private void storeLinkingToFile(Set<PersonPair> linking, String outFilePath) {
        File file = new File(outFilePath);
        try (CSVWriter writer = new CSVWriter(new FileWriter(file),
                CSVWriter.DEFAULT_SEPARATOR,
                CSVWriter.NO_QUOTE_CHARACTER,
                CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                CSVWriter.DEFAULT_LINE_END)) {
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
