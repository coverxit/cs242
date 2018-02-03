# CS242 Project

## Part A

To compile and run the `Crawler`:

```bash
javac -cp "./:./lib/commons-cli-1.4.jar:./lib/jsoup-1.11.2.jar:./lib/sqlite-jdbc-3.21.0.jar" src/edu/ucr/cs242/crawler/*.java
java -cp "./src:./lib/commons-cli-1.4.jar:./lib/jsoup-1.11.2.jar:./lib/sqlite-jdbc-3.21.0.jar" edu.ucr.cs242.crawler.WikiCrawler <jdbc-url>
```

The `jdbc-url` is required to run the Crawler. An example of `jdbc-url`:

```
jdbc:sqlite:pages.db
```

which will create a SQlite database named `pages.db` in the same directory running the command above.