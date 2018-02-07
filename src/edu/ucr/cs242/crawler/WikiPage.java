package edu.ucr.cs242.crawler;

import java.time.LocalDateTime;
import java.util.List;

public class WikiPage {
    private String title;
    private String content;
    private List<String> categories;
    private LocalDateTime lastModify;

    public String getTitle() {
        return title;
    }

    public String getContent() {
        return content;
    }

    public List<String> getCategories() {
        return categories;
    }

    public LocalDateTime getLastModify() {
        return lastModify;
    }

    /**
     * Represent a web page in Wikipedia.
     * @param title      The page title.
     * @param content    The page content.
     * @param categories The categories the page belongs to.
     * @param lastModify The last modification time of the page.
     */
    public WikiPage(String title, String content, List<String> categories, LocalDateTime lastModify) {
        this.title = title;
        this.content = content;
        this.categories = categories;
        this.lastModify = lastModify;
    }
}
