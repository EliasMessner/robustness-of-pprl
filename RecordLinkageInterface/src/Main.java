package RecordLinkageInterface.src;

import org.apache.commons.cli.*;

public class Main {

    static String fromFile, outFile;
    // static String configFile;
    static String l, k, t;

    /**
     * Required command line options:
     * -d / -data: file path to dataset
     * -o / -out: file path to out file (where linked pairs should be stored)
     * -l, -k, -t
     * TODO: l, k, t to be replaced with config file
     */
    public static void main(String[] args) {
        tryGetCommandLineArgumentValues(args);
        PPRLAdapter adapter = new PPRLAdapter();
        adapter.readData(fromFile, l, k, t);
        adapter.getLinking(outFile);
    }

    /**
     * Try to get the command line argument values and assign them to the class fields. If any are missing, print error
     * message and exit program with code 1.
     * @param args Command line arguments
     */
    private static void tryGetCommandLineArgumentValues(String[] args) {
        try {
            CommandLine cmd = getCommandLine(args);
            fromFile = cmd.getOptionValue("d");
            outFile = cmd.getOptionValue("o");
            // String configFile = cmd.getOptionValue("c");
            l = cmd.getOptionValue("l");
            k = cmd.getOptionValue("k");
            t = cmd.getOptionValue("t");
        } catch (ParseException e) {
            System.err.print("Parse error: ");
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    private static CommandLine getCommandLine(String[] args) throws ParseException {
        Options options = new Options();
        options.addRequiredOption("d", "data", true, "Filepath to dataset used for linkage process.");
        options.addRequiredOption("o", "out", true, "Filepath to write linked pairs into.");
        // options.addRequiredOption("c", "config", true, "Filepath to config json file.");
        // TODO read Parameters from config file and add additional layer of abstraction,
        //  i.e. this class should have no knowledge of l, k, t, but just pass the config
        //  file to the adapter
        options.addRequiredOption("l", "bflength", true, "Length of Bloom Filter.");
        options.addRequiredOption("k", "hashcount", true, "Number of hashing iterations on Bloom Filter.");
        options.addRequiredOption("t", "threshold", true, "Threshold for linking process.");
        CommandLineParser parser = new DefaultParser();
        System.out.println("Working Directory = " + System.getProperty("user.dir"));
        return parser.parse(options, args);
    }
}