package edu.ucr.cs242.crawler;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collector;
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
        private final String url;
        private final int depth;

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
    private final Queue<QueueItem> nextUrlQueue = new LinkedList<>();

    private final BlockingQueue<WikiPage> pageQueue = new LinkedBlockingQueue<>();
    private final WriterThread writer;

    /**
     * Construct a crawler thread with given settings.
     *
     * @param threadId       The associated thread id.
     * @param visitedUrls    The set of visited urls; the underlying object should be thread-safe.
     * @param numOfPages     The number of web pages to crawl.
     * @param crawlDepth     The depth of web pages to crawl.
     * @param crawlInterval  The interval of crawling next page, limiting the access rate (milliseconds).
     * @param entryUrl       The url of the entry page.
     * @param crawlHostRegex The url to be crawled should be within this host.
     * @param crawlPathRegex The path of the url should start with this prefix.
     * @param jdbcUrl        The JDBC url to access database.
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

    public void setWriterExitListener(OnWriterExitEventListener exitEventListener) {
        writer.setExitEventListener(exitEventListener);
    }

    private void process(QueueItem nextUrl) {
        Document doc = null;
        URL actualUrl;

        try {
            doc = Jsoup.connect(nextUrl.getUrl()).get();
            // Since Special:Random returns 302, the actual url should be parsed after redirect.
            actualUrl = new URL(doc.location());
        } catch (IOException e) {
            System.out.println("CrawlThread " + threadId + " throws an IOException: " + e.getMessage());
            if (e instanceof MalformedURLException && doc != null) {
                System.out.println("CrawlThread " + threadId + " reports a malformed URL: " + doc.location());
            }
            return;
        }

        // The redirected url may a special page, filter them out first.
        if (actualUrl.getHost().matches(crawlHostRegex) && actualUrl.getPath().matches(crawlPathRegex)) {
            // Remove the anchor part.
            visitedUrls.add(actualUrl.getProtocol() + "://" + actualUrl.getHost() + actualUrl.getFile());

            Element elTitle = doc.getElementById("firstHeading"); // key
            Element elContent = doc.selectFirst("#mw-content-text .mw-parser-output"); // value 1
            Element elCategory = doc.getElementById("mw-normal-catlinks"); // value 2
            Elements elScripts = doc.getElementsByTag("script");

            // Category could be null
            if (elTitle != null && elContent != null) {
                String title = elTitle.text().trim();

                // Remove all reference <sup>s.
                elContent.select("sup[class='reference']").remove();
                // Remove the `edit` links.
                elContent.select("span[class='mw-editsection']").remove();
                // Remove unused tags (table & div).
                Arrays.asList("table", "div").forEach(tag -> elContent.select(tag).remove());
                // Remove empty headings with no paragraphs below it.
                Arrays.asList("h1", "h2", "h3", "h4", "h5", "h6").forEach(
                        tag -> elContent.select(tag + "+" + tag).stream()
                                .map(Element::previousElementSibling)
                                .forEach(Element::remove));
                // The final content can be now generated.
                String content = elContent.children().stream()
                        // We don't need empty elements (that is with no text).
                        .filter(Element::hasText)
                        // Map to its un-encoded text & trim
                        .map(Element::wholeText).map(String::trim)
                        // Collect back to a full string
                        .collect(Collectors.joining("\n"));

                // For categories, we want the text in `#mw-normal-catlinks ul > li`
                List<String> categories = elCategory == null
                        ? new ArrayList<>()
                        : elCategory.select("ul > li").stream()
                        .map(Element::text)
                        .map(String::trim)
                        .collect(Collectors.toList());

                // The last modification timestamp is stored in the 2nd <script> tag from the bottom.
                LocalDateTime lastModify = elScripts.stream().skip(elScripts.size() - 2).limit(1)
                        // Something like ... "timestamp":"what we want" ...
                        .map(el -> {
                            Pattern pattern = Pattern.compile("\"timestamp\":\"([^\"]*)");
                            Matcher matcher = pattern.matcher(el.html());
                            return matcher.find() && matcher.groupCount() == 1 ? matcher.group(1) : null;
                        }).filter(Objects::nonNull)
                        // It is in a format of 20180125092740.
                        .map(timestamp -> LocalDateTime.parse(timestamp, DateTimeFormatter.ofPattern("yyyyMMddHHmmss")))
                        // If not found, use current date time as the last modification.
                        .findFirst().orElse(LocalDateTime.now());

                // Put into writing queue
                try { pageQueue.put(new WikiPage(title, content, categories, lastModify)); }
                // Oops! Something wrong...
                catch (InterruptedException e) { return; }

                // Update the crawled pages count.
                ++crawlCount;

                // Hit the depth limit?
                if (nextUrl.getDepth() >= crawlDepth)
                    return;

                // Push all valid `#mw-content-text > a` into the stack.
                // We want <a> with attribute of href.
                elContent.select("a[href]").stream()
                        .map(a -> a.attr("href"))
                        // Map href into URL object
                        .map(href -> {
                            try { return new URL(actualUrl, href); }
                            catch (MalformedURLException e) { return null; }
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
        }
    }

    private void reportProgress(boolean summary, LocalDateTime startAt) {
        LocalDateTime now = LocalDateTime.now();
        Duration elapsed = Duration.between(startAt, now);
        long hours = elapsed.toHours();
        long minutes = elapsed.toMinutes() % 60;
        long seconds = elapsed.getSeconds() % 60;

        System.out.format("%sCrawlThread %d crawled %d pages, %.2f%% completed. Elapsed time: %02d:%02d:%02d.%n",
                summary ? "Summary: " : "", threadId, crawlCount, crawlCount * 100.0f / numOfPages, hours, minutes, seconds);
    }

    @Override
    public void run() {
        LocalDateTime startAt = LocalDateTime.now();
        System.out.println("CrawlThread " + threadId + " started at " + startAt.toLocalTime() + ". " +
                "Pages to crawl: " + numOfPages + ".");

        writer.start();
        nextUrlQueue.add(new QueueItem(entryUrl, 0));

        // Job finished? or something wrong with writer?
        while (crawlCount < numOfPages && !writer.isInterrupted()) {
            // nextUrlQueue may be empty, since the crawl depth limitation.
            // If so, crawl the entry url again. (Entry url is never put into visitedUrls)
            QueueItem nextUrl = nextUrlQueue.isEmpty() ? new QueueItem(entryUrl, 0) : nextUrlQueue.remove();

            if (!visitedUrls.contains(nextUrl.getUrl())) {
                process(nextUrl);

                // Report crawling progress.
                if (crawlCount % Math.min(numOfPages, WriterThread.BATCH_WRITE_COUNT) == 0) {
                    reportProgress(false, startAt);
                }

                // Be polite.
                try { Thread.sleep(crawlInterval); }
                // We don't care if it is interrupted
                catch (InterruptedException e) { }
            }
        }

        // Check if writer has been interrupted (mostly due to exception).
        // If not, we have to stop writer after the pageQueue is processed.
        if (!writer.isInterrupted()) {
            try {
                synchronized (pageQueue) {
                    while (!pageQueue.isEmpty())
                        pageQueue.wait();
                }
            } catch (InterruptedException e) {
                // Actions are performed in finally block.
            } finally {
                writer.interrupt();
                // Wait WriterThread to exit.
                try { writer.join(); }
                // Who the f**k always interrupts us?
                catch (InterruptedException e) { }
            }

            reportProgress(true, startAt);
        }
    }
}