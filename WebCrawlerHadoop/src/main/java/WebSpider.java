/**
 * Created by ashu on 3/13/2017.
 */

import FileWriters.HDFSFileWriter;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import java.net.URI;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by ashu on 3/11/2017.
 */

public class WebSpider {

    // count the number of times a host has been visited
    private final ConcurrentHashMap<String, AtomicInteger> hostnames = new ConcurrentHashMap<String, AtomicInteger>();
    // keep unique uri's
    private final Set<URI> urlSet = Collections.synchronizedSet(new LinkedHashSet<URI>());
    private String output;
    // a blocking queue / producer consumer pattern
    private final LinkedBlockingDeque<URI> linkQueue;

    CloseableHttpClient client;

    public WebSpider(CloseableHttpClient client){
        this.client = client;
        linkQueue = new LinkedBlockingDeque<>();
    }

    public WebSpider(CloseableHttpClient client, String output) {
        this(client);
        this.output = output;
    }


    // a simple bfs crawl links in the page and create nodes with them
    public void addNewUrls() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(100);
        for (URI uri; (uri = linkQueue.poll(10, TimeUnit.SECONDS)) != null; ) {
            if (!urlSet.add(uri)) continue;
            Thread current = Thread.currentThread();
            String threadName = current.getName();
            current.setName("Crawl: " + uri.toString());

            try {
                Future<StringBuilder> future = pool.submit(new Crawl(uri, linkQueue, client,hostnames));
                StringBuilder sb = future.get();
                HDFSFileWriter.getInstance().writeToFile(sb, pool.isTerminated());
                System.out.println("Queue Size: " + linkQueue.size());
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                current.setName(threadName);
            }
        }

        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.SECONDS);
    }

    public void execute() throws Exception {
        for (int i = 0; i < 5; i++) {
            addNewUrls();
        }
    }




    public static void main(String[] args) throws Exception {

        if(args.length != 2){
            System.out.print("Usage: <Starting link> <output file name>");
            return;
        }

        RequestConfig defaultRequestConfig = RequestConfig.custom()
                .setCookieSpec(CookieSpecs.STANDARD)
                .build();

        PoolingHttpClientConnectionManager poolingHttpClientConnectionManager =
                new PoolingHttpClientConnectionManager();
        poolingHttpClientConnectionManager.setMaxTotal(300);
        poolingHttpClientConnectionManager.setDefaultMaxPerRoute(200);

        CloseableHttpClient client = HttpClients.custom()
                .setConnectionManager(poolingHttpClientConnectionManager)
                .setDefaultRequestConfig(defaultRequestConfig).
                        build();


        // add resource directories
        String[] resourcePaths = {"/home/ashu/hadoop/etc/hadoop/core-site.xml",
                                  "/home/ashu/hadoop/etc/hadoop/hdfs-site.xml"};

        // output file name

        String output = args[1];

        // set HDFS file writer conf
        HDFSFileWriter.setConf(output,resourcePaths);

        String out = "c:\\Users\\ashu\\file5.tsv";
        WebSpider webCrawler = new WebSpider(client);

        // webCrawler.createFile();
        URI uri = new URI(args[0]);
        webCrawler.linkQueue.add(uri);
        webCrawler.execute();

    }

}
