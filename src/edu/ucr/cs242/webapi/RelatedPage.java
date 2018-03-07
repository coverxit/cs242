package edu.ucr.cs242.webapi;

import java.util.List;

public class RelatedPage {
    private String title;
    private String snippet;
    private List<String> categories;
    private String lastModify;

    public String getTitle() {
        return title;
    }

    public String getSnippet() {
        return snippet;
    }

    public List<String> getCategories() {
        return categories;
    }

    public String getLastModify() {
        return lastModify;
    }

    /**
     * Represent a related page.
     * @param title      The page title.
     * @param snippet    The page snippet.
     * @param categories The categories the page belongs to.
     * @param lastModify The last modification time of the page.
     */
    public RelatedPage(String title, String snippet, List<String> categories, String lastModify) {
        this.title = title;
        this.snippet = snippet;
        this.categories = categories;
        this.lastModify = lastModify;
    }
}
