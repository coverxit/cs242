package edu.ucr.cs242.indexing;

import edu.ucr.cs242.OnThreadExitEventListener;
import org.apache.lucene.index.IndexWriter;

import java.sql.Connection;

public class IndexThread extends Thread {
    private final Indexer indexer;
    private final int threadId;
    private final int pageStartIndex;
    private final int numOfPages;
    private final Connection dbConnection;
    private final IndexWriter indexWriter;
    private OnThreadExitEventListener exitEventListener;

    public void setExitEventListener(OnThreadExitEventListener exitEventListener) {
        this.exitEventListener = exitEventListener;
    }

    /**
     * Consturct an indexing thread with given settings.
     * @param indexer        The associated indexer.
     * @param threadId       The associated thread id.
     * @param pageStartIndex The page offset (in the database) to start indexing.
     * @param numOfPages     The number of pages to index.
     * @param dbConnection   The connection to the database.
     * @param indexWriter    The index writer.
     */
    public IndexThread(Indexer indexer, int threadId, int pageStartIndex, int numOfPages,
                       Connection dbConnection, IndexWriter indexWriter) {
        this.indexer = indexer;
        this.threadId = threadId;
        this.pageStartIndex = pageStartIndex;
        this.numOfPages = numOfPages;
        this.dbConnection = dbConnection;
        this.indexWriter = indexWriter;
    }

    @Override
    public void run() {

    }
}
