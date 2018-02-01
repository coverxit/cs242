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
    private static final int BATCH_WRITE_COUNT = 50;
    private static final String SQL_INSERT = "INSERT OR IGNORE INTO pages (title, content, categories) VALUES (?, ?, ?)";

    private final int threadId;
    private final BlockingQueue<WebPage> pageQueue;
    private final Connection dbConnection;

    public WriterThread(int threadId, String jdbcUrl, BlockingQueue<WebPage> pageQueue) throws SQLException {
        this.threadId = threadId;
        this.pageQueue = pageQueue;
        this.dbConnection = DriverManager.getConnection(jdbcUrl);
        this.dbConnection.setAutoCommit(false);
    }

    @Override
    public void run() {
        int bufferedCount = 0;

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
                        int[] result = statement.executeBatch();
                        dbConnection.commit();
                        System.out.format("Thread %d committed %d pages. Most recent one: %s.\n",
                                threadId, Arrays.stream(result).sum(), page.getTitle());
                    }
                } catch (InterruptedException e) {
                    int[] result = statement.executeBatch();
                    dbConnection.commit();
                    dbConnection.close();
                    System.out.format("Thread %d committed %d pages.\n", threadId, Arrays.stream(result).sum());

                    Thread.currentThread().interrupt();
                } catch (SQLException e) {
                    e.printStackTrace();
                    dbConnection.rollback();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
