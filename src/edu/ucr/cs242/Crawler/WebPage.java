package edu.ucr.cs242.Crawler;

import java.util.List;

public class WebPage {
    private String title;
    private String content;
    private List<String> categories;

    public String getTitle() {
        return title;
    }

    public String getContent() {
        return content;
    }

    public List<String> getCategories() {
        return categories;
    }

    /**
     * Represent a web page in Wikipedia.
     * @param title The page title.
     * @param content The page content.
     * @param categories The categories the page belongs to.
     */
    public WebPage(String title, String content, List<String> categories) {
        this.title = title;
        this.content = content;
        this.categories = categories;
    }
}
