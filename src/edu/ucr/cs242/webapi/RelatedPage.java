package edu.ucr.cs242.webapi;

import java.util.List;

public class RelatedPage {
    private String title;
    private String rawTitle;
    private String snippet;
    private List<String> categories;
    private List<String> rawCategories;
    private String lastModify;

    public String getTitle() {
        return title;
    }

    public String getRawTitle() {
        return rawTitle;
    }

    public String getSnippet() {
        return snippet;
    }

    public List<String> getCategories() {
        return categories;
    }

    public List<String> getRawCategories() {
        return rawCategories;
    }

    public String getLastModify() {
        return lastModify;
    }

    /**
     * Represent a related page.
     * @param title         The page title.
     * @param rawTitle      The raw title to the page (no b tags include).
     * @param snippet       The page snippet.
     * @param categories    The categories the page belongs to.
     * @param rawCategories The raw categories the page belongs to (no b tags include).
     * @param lastModify    The last modification time of the page.
     */
    public RelatedPage(String title, String rawTitle, String snippet,
                       List<String> categories, List<String> rawCategories, String lastModify) {
        this.title = title;
        this.rawTitle = rawTitle;
        this.snippet = snippet;
        this.categories = categories;
        this.rawCategories = rawCategories;
        this.lastModify = lastModify;
    }
}
