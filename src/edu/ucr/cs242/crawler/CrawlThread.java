package edu.ucr.cs242.crawler;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

/**
 * The actual thread for crawling, also a producer class.
 */
public class CrawlThread extends Thread {
    private final int threadId;
    private Set<String> visitedUrls;
    private final int numOfPages;
    private final int crawlDepth;
    private final int crawlInterval;
    private final String entryUrl;
    private final String crawlHostRegex;
    private final String crawlPathRegex;

    private class QueueItem {
        private String url;
        private int depth;

        public String getUrl() {
            return url;
        }

        public int getDepth() {
            return depth;
        }

        public QueueItem(String url, int depth) {
            this.url = url;
            this.depth = depth;
        }
    }

    private int crawlCount = 0;
    private Queue<QueueItem> nextUrlQueue = new LinkedList<>();

    private BlockingQueue<WebPage> pageQueue = new LinkedBlockingQueue<>();
    private WriterThread writer;

    /**
     * Construct a crawler thread with given settings.
     * @param threadId The associated thread id.
     * @param visitedUrls The set of visited urls; the underlying object should be thread-safe.
     * @param numOfPages The number of web pages to crawl.
     * @param crawlDepth The depth of web pages to crawl.
     * @param crawlInterval The interval of crawling next page, limiting the access rate (milliseconds).
     * @param entryUrl The url of the entry page.
     * @param crawlHostRegex The url to be crawled should be within this host.
     * @param crawlPathRegex The path of the url should start with this prefix.
     * @param jdbcUrl The JDBC url to access database.
     */
    public CrawlThread(int threadId, Set<String> visitedUrls,
                          int numOfPages, int crawlDepth, int crawlInterval,
                          String entryUrl, String crawlHostRegex, String crawlPathRegex,
                          String jdbcUrl) throws SQLException {
        this.threadId = threadId;
        this.visitedUrls = visitedUrls;
        this.numOfPages = numOfPages;
        this.crawlDepth = crawlDepth;
        this.crawlInterval = crawlInterval;
        this.entryUrl = entryUrl;
        this.crawlHostRegex = crawlHostRegex;
        this.crawlPathRegex = crawlPathRegex;
        this.writer = new WriterThread(threadId, jdbcUrl, pageQueue);
    }

    private void process(QueueItem nextUrl) throws InterruptedException {
        try {
            Document doc = Jsoup.connect(nextUrl.getUrl()).get();

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

                // Hit the depth limit?
                if (nextUrl.getDepth() >= crawlDepth)
                    return;

                // Push all valid `#mw-content-text > a` into the stack.
                elContent.select("a").stream()
                        // We want <a> with attribute of href.
                        .filter(a -> a.hasAttr("href"))
                        .map(a -> a.attr("href"))
                        // Map href into URL object
                        .map(href -> {
                            try {
                                // Absolute link or protocol relative link?
                                return href.contains("://") || href.startsWith("//")
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
                        .forEachOrdered(url -> nextUrlQueue.add(new QueueItem(url, nextUrl.getDepth() + 1)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        writer.start();
        nextUrlQueue.add(new QueueItem(entryUrl, 0));

        try {
            while (crawlCount < numOfPages && !writer.isInterrupted()) {
                QueueItem nextUrl = nextUrlQueue.remove();

                if (!visitedUrls.contains(nextUrl.getUrl())) {
                    process(nextUrl);

                    if (crawlCount % (numOfPages / 100) == 0) {
                        System.out.format("Thread %d crawled %d pages, %.2f%% completed.\n",
                                threadId, crawlCount, crawlCount / (numOfPages / 100.0f));
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