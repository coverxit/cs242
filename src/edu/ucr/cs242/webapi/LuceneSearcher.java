package edu.ucr.cs242.webapi;

import edu.ucr.cs242.Utility;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class LuceneSearcher extends Searcher {
    private final Path indexPath;

    /**
     * Construct a Lucene searcher with given settings.
     * @param jdbcUrl   The JDBC url to the database.
     * @param indexPath The path to Lucene index.
     */
    public LuceneSearcher(String jdbcUrl, Path indexPath) throws SQLException {
        super(jdbcUrl);
        this.indexPath = indexPath;
    }

    // In PhraseQuery, order matters.
    private PhraseQuery buildPhraseQuery(String field, String keyword, int slop) {
        PhraseQuery.Builder builder = new PhraseQuery.Builder();
        builder.setSlop(slop);
        // Split terms by space
        Arrays.stream(keyword.split(" "))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .forEach(s -> builder.add(new Term(field, s)));
        return builder.build();
    }

    private List<RelatedPage> fetchRelatedPages(IndexSearcher searcher, ScoreDoc[] scoreDocs, String category) {
        List<RelatedPage> pages = new ArrayList<>();

        List<String> titles = Arrays.stream(scoreDocs).map(sd -> {
            try { return searcher.doc(sd.doc); }
            catch (IOException e) { return null; }
        }).filter(Objects::nonNull).map(d -> d.get("title")).collect(Collectors.toList());

        int fetchCount = 0;
        while (fetchCount < titles.size()) {
            int localCount = 0;

            int batchSize = Math.min(titles.size() - fetchCount, BATCH_READ_COUNT);
            try (PreparedStatement statement = dbConnection.prepareStatement(Utility.buildBatchSelectPagesSQL(batchSize))) {
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
                System.out.println("LuceneSearcher throws an SQLException.");
                e.printStackTrace();
            }
        }

        return pages;
    }

    @Override
    protected SearchResult searchInternal(String keyword, String category) {
        // Lucene is by default indexed in lower case.
        keyword = keyword.toLowerCase();
        category = category.toLowerCase();

        try {
            Directory directory = FSDirectory.open(indexPath);
            DirectoryReader reader = DirectoryReader.open(directory);
            IndexSearcher searcher = new IndexSearcher(reader);
            BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();

            // Matches in title have a boost of 2
            // Slop of 10 should be fine, for getting additional (lower score) results
            queryBuilder.add(new BoostQuery(buildPhraseQuery("title", keyword, 10), 2), BooleanClause.Occur.SHOULD);
            queryBuilder.add(buildPhraseQuery("content", keyword, 10), BooleanClause.Occur.SHOULD);
            if (!category.isEmpty()) {
                // Category must be exact match
                queryBuilder.add(buildPhraseQuery("categories", category, 0), BooleanClause.Occur.MUST);
            }

            // Only get the top 1000 docs
            TopDocs topDocs = searcher.search(queryBuilder.build(), 1000);
            long hits = topDocs.totalHits;
            List<RelatedPage> pages = fetchRelatedPages(searcher, topDocs.scoreDocs, category);

            reader.close();
            directory.close();
            return new SearchResult(hits, pages);
        } catch (IOException e) {
            System.out.println("LuceneSearcher throws an IOException: " + e.getMessage());
            return null;
        }
    }
}
