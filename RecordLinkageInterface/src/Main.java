import org.apache.commons.cli.*;

public class Main {

    public static void main(String[] args) {
        Options options = new Options();
        options.addRequiredOption("d", "data", true, "Filepath to dataset used for linkage process.");
        options.addRequiredOption("o", "out", true, "Filepath to write linked pairs into.");
        options.addRequiredOption("c", "config", true, "Filepath to config json file.");
        CommandLineParser parser = new DefaultParser();
        try {
            System.out.println("Working Directory = " + System.getProperty("user.dir"));
            CommandLine cmd = parser.parse(options, args);
            String fromFile = cmd.getOptionValue("d");
            String outFile = cmd.getOptionValue("o");
            String configFile = cmd.getOptionValue("c");
            System.out.printf("From: %s \nOut: %s \nConfig: %s \n%n", fromFile, outFile, configFile);
        } catch (ParseException e) {
            System.err.print("Parse error: ");
            System.err.println(e.getMessage());
        }
    }

}