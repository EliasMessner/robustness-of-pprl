package RLInterface;

import org.apache.commons.cli.*;

public class Main {

    static String fromFile, outFile, configFile;

    /**
     * Required command line options:
     * -d / -data: path to dataset
     * -o / -out: path to out file (where linked pairs should be stored)
     * -c / -config: path to config file
     */
    public static void main(String[] args) {
        tryGetCommandLineArgumentValues(args);
        PPRLAdapter adapter = new PPRLAdapter();
        adapter.readData(fromFile, configFile);
        adapter.printLogs(true);
        adapter.getLinking(outFile);
        adapter.printLogs(true);
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
            configFile = cmd.getOptionValue("c");
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
        options.addRequiredOption("c", "config", true, "Filepath to config json file.");
        CommandLineParser parser = new DefaultParser();
        System.out.println("Working Directory = " + System.getProperty("user.dir"));
        return parser.parse(options, args);
    }
}