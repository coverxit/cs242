package edu.ucr.cs242.mapreduce;

import edu.ucr.cs242.Utility;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class NoSQLImporter {
    private final String jsonOutputPath;
    private final String hadoopIndexOutputPath;

    /**
     * Construct an NoSQLImporter with given settings.
     * @param jsonOutputPath        The folder to the JSON output.
     * @param hadoopIndexOutputPath The file name to the Hadoop's index output.
     */
    public NoSQLImporter(String jsonOutputPath, String hadoopIndexOutputPath) {
        this.jsonOutputPath = jsonOutputPath;
        this.hadoopIndexOutputPath = hadoopIndexOutputPath;
    }

    public void start() throws IOException {
        org.iq80.leveldb.Options options = new org.iq80.leveldb.Options();
        options.createIfMissing(true);

        try (DB db = JniDBFactory.factory.open(new File("mixer"), options)) {
            Thread indexImportThread = new IndexImportThread(db, jsonOutputPath, hadoopIndexOutputPath);
            indexImportThread.start();
            Utility.waitThread(indexImportThread);
        }
    }

    private static void printMessage(String message) {
        System.out.println("importer: " + message);
    }

    private static void printUsage() {
        System.out.println("usage: importer [options] <exporter-json-output-path> <hadoop-index-output-path>");
        System.out.println("use -h for a list of possible options");
        System.exit(1);
    }

    private static void printHelp(org.apache.commons.cli.Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("importer [options] <exporter-json-output-path> <hadoop-index-output-path>", options);
        System.out.println();
    }

    public static void main(String[] args) throws IOException {
        org.apache.commons.cli.Options options = new org.apache.commons.cli.Options();
        options.addOption(org.apache.commons.cli.Option.builder("l")
                .longOpt("log-output")
                .argName("FILE NAME")
                .desc("the file to write logs into (default: STDOUT)")
                .numberOfArgs(1)
                .build());

        options.addOption("h", "help", false, "print a synopsis of standard options");

        try {
            CommandLine cmd = new DefaultParser().parse(options, args);
            List<String> argList = cmd.getArgList();

            if (cmd.hasOption("h")) {
                printHelp(options);
                System.exit(0);
            }

            if (argList.isEmpty()) {
                printMessage("SQLExporter's JSON output path is not specified");
                printUsage();
            }

            if (argList.size() <= 1) {
                printMessage("Hadoop's index output path is not specified");
                printUsage();
            }

            String logOutput = cmd.getOptionValue("log-output");
            if (!Utility.openOutputLog(logOutput)) {
                printMessage("invalid log file path");
                printUsage();
            }

            Path jsonOutputPath = Paths.get(argList.get(0));
            if (!Files.exists(jsonOutputPath) || !Files.isDirectory(jsonOutputPath)) {
                printMessage("invalid SQLExporter's JSON output path (not exist or not directory)");
                printUsage();
            }

            Path hadoopIndexOutputPath = Paths.get(argList.get(1));
            if (!Files.exists(hadoopIndexOutputPath) || Files.isDirectory(hadoopIndexOutputPath)) {
                printMessage("invalid Hadoop's index output path (not exist or is a directory)");
                printUsage();
            }

            new NoSQLImporter(jsonOutputPath.toString(), hadoopIndexOutputPath.toString()).start();
        } catch (ParseException e) {
            // Lower the first letter, which as default is an upper letter.
            printMessage(e.getMessage().substring(0, 1).toLowerCase() + e.getMessage().substring(1));
            printHelp(options);
            System.exit(1);
        }
    }
}
