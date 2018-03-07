package edu.ucr.cs242.webapi;

import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

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
