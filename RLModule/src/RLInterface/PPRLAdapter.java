package RLInterface;

import PPRL.*;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class PPRLAdapter implements RecordLinkageI {
    
    private final Launcher launcher;
    private StringBuilder logs;

    public PPRLAdapter() {
        boolean blockingCheat = true;
        boolean parallelBlockingMapCreation = false;
        boolean parallelLinking = false;
        this.launcher = new Launcher(blockingCheat, parallelBlockingMapCreation, parallelLinking, true);
        this.logs = new StringBuilder();
    }

    public PPRLAdapter(boolean blockingCheat, boolean parallelBlockingMapCreation, boolean parallelLinking, boolean alwaysRecreateBloomFilters) {
        this.launcher = new Launcher(blockingCheat, parallelBlockingMapCreation, parallelLinking, alwaysRecreateBloomFilters);
    }

    /**
     * Reads dataset and config file and prepares the launcher.
     */
    public void readData(String fromFile, String configFile, String personBloomFilterMapPath) {
        try {
            Person[] dataSet = getDatasetFromFile(fromFile);
            logs.append(String.format("Dataset size: %d\n", dataSet.length));
            EncoderParams encoderParams = getEncoderParams(configFile);
            MatcherParams matcherParams = getMatcherParams(configFile);
            launcher.prepare(dataSet, encoderParams, matcherParams, personBloomFilterMapPath);
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
        logs.append(String.format("Matches: %d\n", linking.size()));
        try {
            storeLinkingToFile(linking, outFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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

    private MatcherParams getMatcherParams(String configFile) throws IOException, ParseException {
        try (FileReader reader = new FileReader(configFile)) {
            JSONObject jsonObject = (JSONObject) (new JSONParser().parse(reader));
            double t = (double) jsonObject.get("t");
            return new MatcherParams(
                    LinkingMode.POLYGAMOUS,
                    true, t);
        }
    }

    private EncoderParams getEncoderParams(String configFile) throws IOException, ParseException {
        try (FileReader reader = new FileReader(configFile)) {
            JSONObject jsonObject = (JSONObject) (new JSONParser().parse(reader));
            int l = (int) (long) jsonObject.get("l");
            int k = (int) (long) jsonObject.get("k");
            String tokenSalting = (String) jsonObject.get("seed");
            return new EncoderParams(
                    HashingMode.ENHANCED_DOUBLE_HASHING,
                    "SHA-1",
                    "MD5",
                    true,
                    tokenSalting,
                    l, k);
        }
    }

    private void storeLinkingToFile(Set<PersonPair> linking, String outFilePath) throws IOException {
        File file = new File(outFilePath);
        Files.createDirectories(Paths.get(file.getParent()));  // create folder if not exists
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
        }
    }

    public void printLogs(boolean clear) {
        System.out.println(logs.toString());
        if (clear) logs.setLength(0);
    }

}
