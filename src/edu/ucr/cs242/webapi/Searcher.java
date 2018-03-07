package edu.ucr.cs242.webapi;

import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.*;
import java.util.*;
import java.util.function.BiFunction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class Searcher {
    /**
     * The number of records to be batch-read per SQL transaction.
     */
    public static final int BATCH_READ_COUNT = 50;

    /**
     * The identifier for specifying category in the query.
     */
    private static final String CATEGORY_IDENTIFIER = "category:";

    protected final Connection dbConnection;

    protected Searcher(String jdbcUrl) throws SQLException {
        this.dbConnection = DriverManager.getConnection(jdbcUrl);
    }

    private static String buildBatchSelectSQL(int numOfTitles) {
        // In a form of `SELECT title, content, categories, lastModify FROM pages WHERE title IN (?, ?, ?)`
        final String baseSQL = "SELECT title, content, categories, lastModify FROM pages WHERE title IN ";
        return baseSQL + IntStream.range(0, numOfTitles).mapToObj(i -> "?")
                .collect(Collectors.joining(", ", "(", ")"));
    }

    private static String fullTextHighlight(String text, String keyword) {
        List<String> keywordList = Arrays.stream(keyword.split(" "))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                // Guarantee keywords are in lowercase
                .map(String::toLowerCase)
                .collect(Collectors.toList());

        // Idea comes from https://stackoverflow.com/a/12026782
        StringBuilder sbText = new StringBuilder(text);
        StringBuilder sbTextLower = new StringBuilder(text.toLowerCase());
        keywordList.forEach(s -> {
            int idx = 0;

            while ((idx = sbTextLower.indexOf(s, idx)) != -1) {
                sbText.insert(idx, "<b>");
                sbText.insert(idx + "<b>".length() + s.length(), "</b>");
                sbTextLower.replace(idx, idx + s.length(), "<b>" + s + "</b>");
                idx += s.length() + "<b></b>".length();
            }
        });

        return sbText.toString();
    }

    protected List<RelatedPage> fetchRelatedPages(List<String> titles, String keyword, String category,
                                                  BiFunction<String, String, String> fragmentHighlight) {
        // Keep the scored order from Lucene
        Map<String, RelatedPage> pages = new HashMap<>();

        int fetchCount = 0;
        while (fetchCount < titles.size()) {
            int localCount = 0;

            int batchSize = Math.min(titles.size() - fetchCount, BATCH_READ_COUNT);
            try (PreparedStatement statement = dbConnection.prepareStatement(buildBatchSelectSQL(batchSize))) {
                for (int i = 1; i <= batchSize; i++) {
                    statement.setString(i, titles.get(fetchCount + i - 1));
                }

                try (ResultSet result = statement.executeQuery()) {
                    while (result.next()) {
                        String title = result.getString("title");
                        String content = fragmentHighlight.apply(result.getString("content"), keyword);
                        List<String> categories =
                                Arrays.stream(result.getString("categories").split(Pattern.quote("|")))
                                .map(s -> fullTextHighlight(s, category))
                                .collect(Collectors.toList());
                        String lastMod = result.getString("lastModify");

                        pages.put(title, new RelatedPage(fullTextHighlight(title, keyword), content, categories, lastMod));
                        ++localCount;
                    }
                }

                fetchCount += localCount;
            } catch (SQLException e) {
                System.out.println("Searcher::fetchRelatedPages throws an SQLException.");
                e.printStackTrace();
            }
        }

        return titles.stream().map(pages::get).collect(Collectors.toList());
    }

    public final JSONObject search(String query) {
        String keyword, category;

        int pos = query.indexOf(CATEGORY_IDENTIFIER);
        if (pos != -1) {
            keyword = query.substring(0, pos).trim();
            category = query.substring(pos + CATEGORY_IDENTIFIER.length()).trim();
        } else {
            keyword = query;
            category = "";
        }

        // Convert to lower case, since both Lucene and our algorithm is indexed in lower case.
        keyword = keyword.toLowerCase();
        category = category.toLowerCase();

        SearchResult result = searchInternal(keyword, category);
        JSONObject response = new JSONObject().put("hits", result.getNumOfHits());

        JSONArray array = new JSONArray();
        result.getRelatedPages().forEach(p -> {
            JSONObject obj = new JSONObject();
            obj.put("title", p.getTitle());
            obj.put("snippet", p.getSnippet());
            obj.put("categories", new JSONArray(p.getCategories()));
            obj.put("lastModify", p.getLastModify());
            array.put(obj);
        });
        response.put("pages", array);

        return response;
    }

    protected abstract SearchResult searchInternal(String keyword, String category);
}
