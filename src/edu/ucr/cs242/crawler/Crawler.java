package edu.ucr.cs242.crawler;

import org.apache.commons.cli.*;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Crawler {
    private final int numOfThreads;
    private final int numOfPages;
    private final int crawlDepth;
    private final int crawlInterval;
    private final String entryUrl;
    private final String crawlHostRegex;
    private final String crawlPathRegex;
    private final String jdbcUrl;

    private Set<String> visitedUrls = ConcurrentHashMap.newKeySet();

    /**
     * Construct a crawler with given settings.
     * @param numOfThreads The number of threads for crawling.
     * @param numOfPages The number of web pages to crawl.
     * @param crawlDepth The depth of web pages to crawl.
     * @param crawlInterval The interval of crawling next page, limiting the access rate (milliseconds).
     * @param entryUrl The url of the entry page.
     * @param crawlHostRegex The url to be crawled should be within this host.
     * @param crawlPathRegex The path of the url should start with this prefix.
     * @param jdbcUrl The JDBC url to access database.
     */
    public Crawler(int numOfThreads, int numOfPages, int crawlDepth, int crawlInterval,
                   String entryUrl, String crawlHostRegex, String crawlPathRegex,
                   String jdbcUrl) {
        this.numOfThreads = numOfThreads;
        this.numOfPages = numOfPages;
        this.crawlDepth = crawlDepth;
        this.crawlInterval = crawlInterval;
        this.entryUrl = entryUrl;
        this.crawlHostRegex = crawlHostRegex;
        this.crawlPathRegex = crawlPathRegex;
        this.jdbcUrl = jdbcUrl;
    }

    /**
     * Start the crawler.
     * @throws InterruptedException
     */
    public void start() throws InterruptedException {
        LocalDateTime startAt = LocalDateTime.now();
        CrawlThread[] threads = new CrawlThread[numOfThreads];
        AtomicInteger committedCount = new AtomicInteger(0);

        System.out.println("Crawler started at " + startAt.toLocalTime() + ". " +
                "Pages to crawl: " + numOfPages + ".");

        for (int i = 0; i < numOfThreads; i++) {
            try {
                threads[i] = new CrawlThread(i, visitedUrls, numOfPages / numOfThreads,
                        crawlDepth, crawlInterval, entryUrl, crawlHostRegex, crawlPathRegex, jdbcUrl);
                threads[i].setWriterEventListener(committedCount::addAndGet);
                threads[i].start();
            } catch (SQLException e) {
                threads[i] = null;
                e.printStackTrace();
            }
        }

        for (int i = 0; i < numOfThreads; i++) {
            if (threads[i] != null) {
                threads[i].join();
            }
        }

        LocalDateTime now = LocalDateTime.now();
        Duration elapsed = Duration.between(startAt, now);
        long hours = elapsed.toHours();
        long minutes = elapsed.toMinutes() % 60;
        long seconds = elapsed.getSeconds() % 60;

        System.out.format("Summary: Crawler committed %d pages in total. ", committedCount.get());
        System.out.format("Elapsed time: %02d:%02d:%02d.\n", hours, minutes, seconds);
    }

    /**
     * Initialize database, creating necessary tables.
     * @param jdbcUrl The JDBC connection string.
     * @return Whether the table creation succeeded.
     */
    private static boolean initializeDatabase(String jdbcUrl) {
        final String SQL_CREATE =
                "CREATE TABLE IF NOT EXISTS pages (" +
                "title TEXT PRIMARY KEY, " +
                "content TEXT NOT NULL, " +
                "categories TEXT)";

        try (Connection dbConnection = DriverManager.getConnection(jdbcUrl);
             Statement query = dbConnection.createStatement()) {
            query.execute(SQL_CREATE);
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    private static void printUsage() {
        System.out.println("usage: crawler [options] <jdbc-url>");
        System.out.println("use -h for a list of possible options");
        System.exit(1);
    }

    private static void printMessage(String message) {
        System.out.println("crawler: " + message);
    }

    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("crawler [options] <jdbc-url>", options);
        System.out.println();
    }

    public static void main(String[] args) throws InterruptedException {
        // Default values
        final int NUMBER_OF_THREADS = 10;
        final int NUMBER_OF_PAGES = 200000;
        final int CRAWL_DEPTH = 10;
        final int CRAWL_INTERVAL = 500;
        final String ENTRY_URL = "https://en.wikipedia.org/wiki/Special:Random";
        final String CRAWL_HOST_REGEX = "^en.wikipedia.org$";
        final String CRAWL_PATH_REGEX = "^/wiki/[^:]*$"; // Special pages (such as Help:Category) are not crawled

        Options options = new Options();
        options.addOption(Option.builder("t")
                        .longOpt("threads")
                        .argName("NUM OF THREADS")
                        .desc("the number of threads for crawling (default: " + NUMBER_OF_THREADS + ")")
                        .numberOfArgs(1)
                        .build());

        options.addOption(Option.builder("c")
                        .longOpt("pages")
                        .argName("NUM OF PAGES")
                        .desc("the number of web pages to crawl (default: " + NUMBER_OF_PAGES + ")")
                        .numberOfArgs(1)
                        .build());

        options.addOption(Option.builder("d")
                        .longOpt("depth")
                        .argName("DEPTH")
                        .desc("the depth of web pages to crawl (default: " + CRAWL_DEPTH + ")")
                        .numberOfArgs(1)
                        .build());

        options.addOption(Option.builder("i")
                        .longOpt("interval")
                        .argName("INTERVAL")
                        .desc("the interval (milliseconds) of crawling next page, " +
                                "limiting the access rate (default: " + CRAWL_INTERVAL + ")")
                        .numberOfArgs(1)
                        .build());

        options.addOption(Option.builder("u")
                        .longOpt("entry-url")
                        .argName("ENTRY URL")
                        .desc("the url of the entry page (default: " + ENTRY_URL + ")")
                        .numberOfArgs(1)
                        .build());

        options.addOption(Option.builder("H")
                        .longOpt("host-regex")
                        .argName("HOST REGEX")
                        .desc("the url to be crawled should be within this host (default: " + CRAWL_HOST_REGEX + ")")
                        .numberOfArgs(1)
                        .build());

        options.addOption(Option.builder("P")
                        .longOpt("path-regex")
                        .argName("PATH REGEX")
                        .desc("the path of the url should start with this prefix (default: " + CRAWL_PATH_REGEX + ")")
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
                printMessage("no jdbc url");
                printUsage();
            }

            String jdbcUrl = argList.get(0);
            if (!initializeDatabase(jdbcUrl)) {
                printMessage("invalid jdbc url");
                printUsage();
            }

            try {
                int numOfThreads = Integer.parseInt(cmd.getOptionValue("threads", String.valueOf(NUMBER_OF_THREADS)));
                int numOfPages = Integer.parseInt(cmd.getOptionValue("pages", String.valueOf(NUMBER_OF_PAGES)));
                int crawlDepth = Integer.parseInt(cmd.getOptionValue("depth", String.valueOf(CRAWL_DEPTH)));
                int crawlInterval = Integer.parseInt(cmd.getOptionValue("interval", String.valueOf(CRAWL_INTERVAL)));

                // Test if valid url
                String entryUrl = new URL(cmd.getOptionValue("entry-url", ENTRY_URL)).toString();
                String crawlHostRegex = cmd.getOptionValue("host-regex", CRAWL_HOST_REGEX);
                String crawlPathRegex = cmd.getOptionValue("path-regex", CRAWL_PATH_REGEX);

                new Crawler(numOfThreads, numOfPages, crawlDepth, crawlInterval,
                        entryUrl, crawlHostRegex, crawlPathRegex, jdbcUrl).start();
            } catch (NumberFormatException | MalformedURLException e) {
                printMessage("invalid option(s)");
                printHelp(options);
                System.exit(1);
            }
        } catch (ParseException e) {
            // Lower the first letter, which as default is an upper letter.
            printMessage(e.getMessage().substring(0, 1).toLowerCase() + e.getMessage().substring(1));
            printHelp(options);
            System.exit(1);
        }
    }
}
