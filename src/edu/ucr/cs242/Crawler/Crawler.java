package edu.ucr.cs242.Crawler;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Objects;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

public class Crawler {
    /**
     * The number of threads for crawling.
     */
    public static final int NUMBER_OF_THREADS = 1;

    /**
     * The depth of web pages to crawl.
     */
    public static final int CRAWL_DEPTH = 5; // Integer.MAX_VALUE

    /**
     * The interval of crawling next page, limiting the access rate.
     */
    public static final int CRAWL_INTERVAL = 200; // ms

    /**
     * The url of the entry page.
     */
    public static final String ENTRY_URL = "https://en.wikipedia.org/wiki/Special:Random";

    private int numOfThreads = NUMBER_OF_THREADS;
    private int crawlDepth = CRAWL_DEPTH;
    private int crawlInterval = CRAWL_INTERVAL;
    private String entryUrl = ENTRY_URL;

    private Set<String> visitedURLs = ConcurrentHashMap.newKeySet();

    public Crawler() {

    }

    public Crawler(int numOfThreads, int crawlDepth, int crawlInterval, String entryUrl) {
        this.numOfThreads = numOfThreads;
        this.crawlDepth = crawlDepth;
        this.crawlInterval = crawlInterval;
        this.entryUrl = entryUrl;
    }

    public void start() {
        for (int i = 0; i < NUMBER_OF_THREADS; i++) {
            new Thread(this::threadEntryPoint).start();
        }
    }

    protected void threadEntryPoint() {
        try {
            int currentDepth = 0;
            Stack<String> urls = new Stack<>();

            urls.push(ENTRY_URL);
            while (!urls.empty() && currentDepth++ < crawlDepth) {
                String nextUrl = urls.pop();

                if (visitedURLs.contains(nextUrl))
                    continue;

                try {
                    Document doc = Jsoup.connect(nextUrl).get();
                    Element title = doc.getElementById("firstHeading");

                    Element content = doc.getElementById("bodyContent");

                    content.select("a").stream()
                            .map(e -> e.attr("href"))
                            .filter(href -> !href.isEmpty())
                            .map(href -> {
                                try {
                                    return new URL(href);
                                }
                                catch (MalformedURLException e) {
                                    System.out.println("href: " + href);
                                    e.printStackTrace();
                                    return null;
                                }
                            })
                            .filter(Objects::nonNull);

                    // Since Special:Random returns 302, the actual url should be parsed after redirect.
                    String actualUrl = doc.location();
                    visitedURLs.add(actualUrl);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                Thread.sleep(CRAWL_INTERVAL);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) {
        new Crawler().start();
    }
}
