package edu.ucr.cs242.Crawler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

/**
 * The consumer class, to write data into database.
 */
public class WriterThread extends Thread {
    /**
     * The number of records to be batch-written per SQL transaction.
     */
    public static final int BATCH_WRITE_COUNT = 50;
    /**
     * The SQL insert statement.
     */
    public static final String SQL_INSERT = "INSERT OR IGNORE INTO pages (title, content, categories) VALUES (?, ?, ?)";

    private final int threadId;
    private final BlockingQueue<WebPage> pageQueue;

    private Connection dbConnection;

    /**
     * Construct a writer thread, with given settings.
     * @param threadId The associated thread id.
     * @param jdbcUrl The JDBC connection string.
     * @param pageQueue The producer-consumer queue.
     * @throws SQLException
     */
    public WriterThread(int threadId, String jdbcUrl, BlockingQueue<WebPage> pageQueue) throws SQLException {
        this.threadId = threadId;
        this.pageQueue = pageQueue;

        this.dbConnection = DriverManager.getConnection(jdbcUrl);
        this.dbConnection.setAutoCommit(false);
    }

    @Override
    public void run() {
        int bufferedCount = 0;
        int committedCount = 0;

        try {
            PreparedStatement statement = dbConnection.prepareStatement(SQL_INSERT);

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    WebPage page = pageQueue.take();

                    statement.setString(1, page.getTitle());
                    statement.setString(2, page.getContent());
                    statement.setString(3, page.getCategories().stream().collect(Collectors.joining("|")));
                    statement.addBatch();

                    if (++bufferedCount % BATCH_WRITE_COUNT == 0) {
                        int sum = Arrays.stream(statement.executeBatch()).sum();
                        dbConnection.commit();
                        committedCount += sum;

                        System.out.format("Thread %d committed %d pages. Most recent one: %s.\n",
                                threadId, sum, page.getTitle());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            int sum = Arrays.stream(statement.executeBatch()).sum();
            dbConnection.commit();
            committedCount += sum;
        } catch (Exception e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();

            try { dbConnection.rollback(); }
            catch (SQLException _e) { _e.printStackTrace(); }
        } finally {
            System.out.format("Summary: thread %d committed %d pages.\n", threadId, committedCount);

            try { dbConnection.close(); }
            catch (SQLException _e) { _e.printStackTrace(); }
        }
    }
}
