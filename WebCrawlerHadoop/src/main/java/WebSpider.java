/**
 * Created by ashu on 3/13/2017.
 */

import FileWriters.Conf;
import Pagerank.PageRank;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by ashu on 3/11/2017.
 */

public class WebSpider {

    // count the number of times a host has been visited
    private final ConcurrentHashMap<String, AtomicInteger> hostnames = new ConcurrentHashMap<String, AtomicInteger>();
    // keep unique uri's
    private final Set<URI> urlSet = Collections.synchronizedSet(new LinkedHashSet<URI>());
    // a blocking queue / producer consumer pattern
    private final LinkedBlockingDeque<URI> linkQueue;
    private CloseableHttpClient client;
    private AtomicInteger count;
    private int linkSize;
    private AtomicLong fileCounter;

    public WebSpider(CloseableHttpClient client, int linkSize) {
        this.client = client;
        linkQueue = new LinkedBlockingDeque<>();
        count = new AtomicInteger();
        this.linkSize = linkSize;
        fileCounter = new AtomicLong();
    }

    // a simple bfs crawl links in the page and create nodes with them
    public void addNewUrls() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(100);
        for (URI uri; (uri = linkQueue.poll(160, TimeUnit.SECONDS)) != null; ) {
            if (!urlSet.add(uri)) continue;
            Thread current = Thread.currentThread();
            String threadName = current.getName();
            current.setName("Crawl: " + uri.toString());
            count.incrementAndGet();
            try {
                pool.execute(new Crawl(uri, linkQueue, client, hostnames, count,linkSize,fileCounter));
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                current.setName(threadName);
            }
        }

        shutdown(pool);
    }

    // stop threads
    private void shutdown(ExecutorService pool) {
        System.out.println("Shutodown initiated");
        pool.shutdown();
        try {
            if (!pool.awaitTermination(120, TimeUnit.MINUTES)) {
                pool.shutdownNow(); // terminate every thread
                if (!pool.awaitTermination(2, TimeUnit.MINUTES)) {
                    System.out.println("Didnt terminate"); // didn't terminate
                }
            }

        } catch (InterruptedException e) {
            pool.shutdownNow(); // try again
            Thread.currentThread().interrupt();
        }
    }


    public void execute() throws Exception {
        addNewUrls();
    }


    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.out.println("Usage: <Starting link>  <Iterations> <total links>");
            return;
        }

        // set cookie configuration
        RequestConfig defaultRequestConfig = RequestConfig.custom()
                .setCookieSpec(CookieSpecs.STANDARD)
                .build();

        // create a pooling http client manager
        PoolingHttpClientConnectionManager poolingHttpClientConnectionManager =
                new PoolingHttpClientConnectionManager();
        poolingHttpClientConnectionManager.setMaxTotal(300);
        poolingHttpClientConnectionManager.setDefaultMaxPerRoute(200);

        // add cookie and pooling configs to http client
        CloseableHttpClient client = HttpClients.custom()
                .setConnectionManager(poolingHttpClientConnectionManager)
                .setDefaultRequestConfig(defaultRequestConfig).
                        build();

        // start web crawling
        WebSpider webCrawler = new WebSpider(client, Integer.parseInt(args[2]));
        URI uri = new URI(args[0]);
        webCrawler.linkQueue.add(uri);
        webCrawler.execute();


        Configuration conf = Conf.getConf();
        FileSystem fs =  null;

        try {
            fs = FileSystem.get(conf);
        }catch (IOException e){
            System.out.print("Couldn't get filesystem" + e);
        }

        String output = "output.link";

        Path workingDir = fs.getHomeDirectory();
        Path out = new Path("/WebCrawler/iter0/" + output);
        Path input = new Path("/WebCrawler/links/");

        Path inputPath = Path.mergePaths(workingDir, input);
        Path outputPath = Path.mergePaths(workingDir, out);

		// merge all the files to a single file
        FileUtil.copyMerge(fs,inputPath,fs,outputPath,true,conf,"");
		
		System.out.println("Starting page rank...");
        PageRank pageRank = new PageRank();
        pageRank.run(Integer.parseInt(args[1]));

        System.out.println("Finished!!!");

    }

}
