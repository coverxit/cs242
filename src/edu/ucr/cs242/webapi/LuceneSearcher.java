package edu.ucr.cs242.webapi;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
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

    private BooleanQuery buildKeywordQuery(String field, String keyword, BooleanClause.Occur occur) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        // Split terms by space
        Arrays.stream(keyword.split(" "))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .forEach(s -> builder.add(new TermQuery(new Term(field, s)), occur));
        return builder.build();
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

            Query titleQuery = new BooleanQuery.Builder()
                    .add(new BoostQuery(new BooleanQuery.Builder()
                            .add(buildKeywordQuery("title", keyword, BooleanClause.Occur.MUST), BooleanClause.Occur.SHOULD)
                            .add(buildPhraseQuery("title", keyword, 0), BooleanClause.Occur.SHOULD)
                            .build(), 2.0f), BooleanClause.Occur.SHOULD)
                    .add(new BoostQuery(new BooleanQuery.Builder()
                            .add(buildPhraseQuery("title", keyword, 0), BooleanClause.Occur.MUST_NOT)
                            .add(buildKeywordQuery("title", keyword, BooleanClause.Occur.SHOULD), BooleanClause.Occur.MUST)
                            .build(), 0.1f), BooleanClause.Occur.SHOULD)
                    .build();
            queryBuilder.add(titleQuery, BooleanClause.Occur.MUST);

            queryBuilder.add(new BoostQuery(buildKeywordQuery("content", keyword, BooleanClause.Occur.SHOULD), 0.1f), BooleanClause.Occur.MUST);
            if (!category.isEmpty()) {
                // Category must be exact match
                queryBuilder.add(buildPhraseQuery("categories", category, 0), BooleanClause.Occur.MUST);
            }

            System.out.println(queryBuilder.build().toString());

            // Only get the top 1000 docs
            TopDocs topDocs = searcher.search(queryBuilder.build(), 1000);
            long hits = topDocs.totalHits;
            List<String> titles = Arrays.stream(topDocs.scoreDocs).map(sd -> {
                try { return searcher.doc(sd.doc); }
                catch (IOException e) { return null; }
            }).filter(Objects::nonNull).map(d -> d.get("title")).collect(Collectors.toList());
            List<RelatedPage> pages = fetchRelatedPages(titles, keyword, category);

            reader.close();
            directory.close();
            return new SearchResult(hits, pages);
        } catch (IOException e) {
            System.out.println("LuceneSearcher throws an IOException: " + e.getMessage());
            return null;
        }
    }
}
