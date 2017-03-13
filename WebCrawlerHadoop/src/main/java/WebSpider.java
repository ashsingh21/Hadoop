/**
 * Created by ashu on 3/13/2017.
 */

import FileWriters.HDFSFileWriter;
import Pagerank.PageRank;
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
    private final LinkedBlockingQueue<URI> linkQueue;
    private CloseableHttpClient client;
    private AtomicInteger count;
    private int iterations;

    public WebSpider(CloseableHttpClient client, int iterations) {
        this.client = client;
        linkQueue = new LinkedBlockingQueue<>();
        count = new AtomicInteger();
        this.iterations = iterations;
    }

    public WebSpider(CloseableHttpClient client,int iterations, String output) {
        this(client,iterations);
        this.output = output;
    }


    // a simple bfs crawl links in the page and create nodes with them
    public void addNewUrls() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(100);
        for (URI uri; (uri = linkQueue.poll(10, TimeUnit.SECONDS)) != null; ) {
            System.out.println("Count: " + count.get());
            if(count.incrementAndGet() > iterations){
                System.out.println("Initiating shutdown");
                shutdown(pool);
            }
            if (!urlSet.add(uri)) continue;
            Thread current = Thread.currentThread();
            String threadName = current.getName();
            current.setName("Crawl: " + uri.toString());

            try {
                Future<StringBuilder> future = pool.submit(new Crawl(uri, linkQueue, client, hostnames,count, iterations));
                StringBuilder sb = future.get();
                HDFSFileWriter.getInstance().writeToFile(sb, pool.isTerminated());
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                current.setName(threadName);
            }
        }




    }

    private void shutdown(ExecutorService pool){
        pool.shutdown();
        try {
            if (!pool.awaitTermination(5, TimeUnit.MINUTES)) {
                pool.shutdownNow(); // terminate every thread
                if (!pool.awaitTermination(60, TimeUnit.SECONDS))
                    System.out.println("Didnt terminate"); // didn't terminate
            }

        } catch (InterruptedException e) {
            pool.shutdownNow(); // try again
            Thread.currentThread().interrupt(); // interrup threads if not interrupted
        }
    }


    public void execute() throws Exception {
        for (int i = 0; i < 3; i++) {
            addNewUrls();
        }
    }


    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.out.println("Usage: <Starting link> <total Links> <iterations>");
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


        WebSpider webCrawler = new WebSpider(client, Integer.parseInt(args[1]));


        URI uri = new URI(args[0]);
        webCrawler.linkQueue.add(uri);
        webCrawler.execute();

        PageRank pageRank = new PageRank();
        pageRank.run(Integer.parseInt(args[2]));

    }

}
