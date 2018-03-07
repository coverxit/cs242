package edu.ucr.cs242.webapi;

import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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

    protected List<RelatedPage> fetchRelatedPages(List<String> titles, String keyword, String category) {
        List<RelatedPage> pages = new ArrayList<>();

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
                        String content = result.getString("content").substring(0, 10);
                        List<String> categories = Arrays.asList(result.getString("categories").split(Pattern.quote("|")));
                        String lastMod = result.getString("lastModify");

                        pages.add(new RelatedPage(title, content, categories, lastMod));
                        ++localCount;
                    }
                }

                fetchCount += localCount;
            } catch (SQLException e) {
                System.out.println("Searcher::fetchRelatedPages throws an SQLException.");
                e.printStackTrace();
            }
        }

        return pages;
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

        SearchResult result = searchInternal(keyword, category);
        JSONObject response = new JSONObject().put("hits", result.getNumOfHits());

        JSONArray array = new JSONArray();
        result.getRelatedPages().forEach(p -> {
            JSONObject obj = new JSONObject();
            obj.put("title", p.getTitle());
            //obj.put("snippet", p.getSnippet());
            //obj.put("categories", new JSONArray(p.getCategories()));
            //obj.put("lastModify", p.getLastModify());
            array.put(obj);
        });
        response.put("pages", array);

        return response;
    }

    protected abstract SearchResult searchInternal(String keyword, String category);
}
