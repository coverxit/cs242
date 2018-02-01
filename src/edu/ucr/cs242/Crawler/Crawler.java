package edu.ucr.cs242.Crawler;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

public class Crawler {
    private final int numOfThreads;
    private final int numOfPages;
    private final int crawlInterval;
    private final String entryUrl;
    private final String crawlHostRegex;
    private final String crawlPathRegex;
    private final String jdbcUrl;

    private Set<String> visitedUrls = ConcurrentHashMap.newKeySet();

    /**
     * Construct a Crawler with given settings.
     * @param numOfThreads The number of threads for crawling.
     * @param numOfPages The number of web pages to crawl.
     * @param crawlInterval The interval of crawling next page, limiting the access rate (milliseconds).
     * @param entryUrl The url of the entry page.
     * @param crawlHostRegex The url to be crawled should be within this host.
     * @param crawlPathRegex The path of the url should start with this prefix.
     * @param jdbcUrl The JDBC url to access database.
     */
    public Crawler(int numOfThreads, int numOfPages, int crawlInterval,
                   String entryUrl, String crawlHostRegex, String crawlPathRegex,
                   String jdbcUrl) {
        this.numOfThreads = numOfThreads;
        this.numOfPages = numOfPages;
        this.crawlInterval = crawlInterval;
        this.entryUrl = entryUrl;
        this.crawlHostRegex = crawlHostRegex;
        this.crawlPathRegex = crawlPathRegex;
        this.jdbcUrl = jdbcUrl;
    }

    public void start() throws InterruptedException, SQLException {
        Thread[] threads = new Thread[numOfThreads];

        for (int i = 0; i < numOfThreads; i++) {
            threads[i] = new CrawlThread(i, numOfPages / numOfThreads);
            threads[i].start();

            System.out.println("Thread " + i + " started.");
        }

        for (int i = 0; i < numOfThreads; i++) {
            threads[i].join();
        }
    }

    /**
     * The actual thread for crawling, also a producer class.
     */
    protected class CrawlThread extends Thread {
        private final int threadId;
        private final int numberOfPages;

        private Queue<String> nextUrlQueue = new LinkedList<>();

        private BlockingQueue<WebPage> pageQueue = new LinkedBlockingQueue<>();
        private WriterThread writer;

        protected CrawlThread(int threadId, int numberOfPages) throws SQLException {
            this.threadId = threadId;
            this.numberOfPages = numberOfPages;
            this.writer = new WriterThread(threadId, jdbcUrl, pageQueue);
        }

        @Override
        public void run() {
            int crawlCount = 0;

            writer.start();
            nextUrlQueue.add(entryUrl);

            try {
                while (crawlCount < numberOfPages) {
                    String nextUrl = nextUrlQueue.remove();

                    if (!visitedUrls.contains(nextUrl)) {
                        try {
                            Document doc = Jsoup.connect(nextUrl).get();

                            // Since Special:Random returns 302, the actual url should be parsed after redirect.
                            URL actualUrl = new URL(doc.location());
                            visitedUrls.add(doc.location());

                            Element elTitle = doc.getElementById("firstHeading"); // key
                            Element elContent = doc.select("#mw-content-text .mw-parser-output").first(); // value 1
                            Element elCategory = doc.getElementById("mw-normal-catlinks"); // value 2

                            // Category could be null
                            if (elTitle != null && elContent != null) {
                                String title = elTitle.text().trim();
                                String content = elContent.text().trim();

                                // We want the text in `#mw-normal-catlinks ul > li`
                                List<String> categories = elCategory == null
                                        ? new ArrayList<>()
                                        : elCategory.select("ul > li").stream()
                                        .map(Element::text)
                                        .map(String::trim)
                                        .collect(Collectors.toList());

                                // Put into writing queue
                                pageQueue.put(new WebPage(title, content, categories));
                                ++crawlCount;

                                // Push all valid `#mw-content-text > a` into the stack.
                                elContent.select("a").stream()
                                        // We want <a> with attribute of href.
                                        .filter(a -> a.hasAttr("href"))
                                        .map(a -> a.attr("href"))
                                        // Map href into URL object
                                        .map(href -> {
                                            try {
                                                // Absolute link?
                                                return href.contains("://")
                                                        ? new URL(href)
                                                        : new URL(actualUrl.getProtocol(), actualUrl.getHost(), actualUrl.getPort(), href);
                                            } catch (MalformedURLException e) {
                                                return null;
                                            }
                                        }).filter(Objects::nonNull)
                                        // We only want the link inside a given host and the path meets some requirement.
                                        .filter(url -> url.getHost().matches(crawlHostRegex) && url.getPath().matches(crawlPathRegex))
                                        // Reconstruct the URL, remove the anchor part.
                                        // There may be some duplicate URLs after this processing.
                                        .map(url -> url.getProtocol() + "://" + url.getHost() + url.getFile())
                                        .distinct()
                                        // Check if the URL has already stored in the stack.
                                        .filter(url -> !visitedUrls.contains(url))
                                        .forEachOrdered(url -> nextUrlQueue.add(url));
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        Thread.sleep(crawlInterval);
                    }
                }

                writer.interrupt();
                writer.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    protected void initializeDatabase() throws SQLException {
        Connection dbConnection = DriverManager.getConnection(jdbcUrl);

        Statement query = dbConnection.createStatement();
        query.execute("CREATE TABLE IF NOT EXISTS pages (title TEXT PRIMARY KEY, content TEXT NOT NULL, categories TEXT)");

        dbConnection.close();
    }

    public static void main(String[] args) throws InterruptedException, SQLException {
        /*
        Options options = new Options();

        options.addOption(Option.builder("n")
                        .longOpt("thread")
                        .argName("NUM OF THREADS")
                        .desc("the number of threads for crawling")
                        .numberOfArgs(1)
                        .build());

        options.addOption(Option.builder("d")
                        .longOpt("depth")
                        .argName("DEPTH")
                        .desc("The depth of web pages to crawl")
                        .numberOfArgs(1)
                        .build());

        options.addOption(Option.builder("i")
                        .longOpt("interval")
                        .argName("INTERVAL")
                        .desc("the interval of crawling next page, limiting the access rate (milliseconds)")
                        .numberOfArgs(1)
                        .build());
        */

        final int NUMBER_OF_THREADS = 10;
        final int NUMBER_OF_PAGES = 200000;
        final int CRAWL_INTERVAL = 500;
        final String ENTRY_URL = "https://en.wikipedia.org/wiki/Special:Random";
        final String CRAWL_HOST_REGEX = "^en.wikipedia.org$";
        final String CRAWL_PATH_REGEX = "^/wiki/[^:]*$"; // Special pages (such as Help:Category) are not crawled
        final String JDBC_URL = "jdbc:sqlite:test.db";

        final Crawler crawler = new Crawler(NUMBER_OF_THREADS, NUMBER_OF_PAGES, CRAWL_INTERVAL,
                ENTRY_URL, CRAWL_HOST_REGEX, CRAWL_PATH_REGEX, JDBC_URL);

        crawler.initializeDatabase();
        crawler.start();
    }
}
