import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by ashu on 3/11/2017.
 */

public class WebCrawler {

    private final HttpClient client;
    private final LinkedBlockingQueue<URI> linkQueue = new LinkedBlockingQueue<URI>(); // a blocking list for URI's
    private String path;
    private PrintWriter bw;
    // count the number of times a host has been visited
    private final ConcurrentHashMap<String, AtomicInteger> hostnames = new ConcurrentHashMap<String, AtomicInteger>();
    // keep unique uri's
    private final Set<URI> urlSet = Collections.synchronizedSet(new LinkedHashSet<URI>());
    private int threadCount;
    private boolean fileCreated = false;

    public WebCrawler(CloseableHttpClient client, int threadCount) {
        this.client = client;
        this.threadCount = threadCount;
    }

    public boolean createFile(String output, String[] resourcePaths) {
        this.path = path;
        File file = null;
        try {
         /*   file = new File(path);
            if (!file.exists()) {
                file.createNewFile();
            }*/
            bw = HDFSFileWriter.get(output,resourcePaths);
         //   bw = new BufferedWriter(new FileWriter(path, true));
            fileCreated = true;
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            fileCreated = false;
        }
        return false;
    }

    // a simple bfs crawl links in the page and create nodes with them
    public void addNewUrls() throws Exception {
        for (URI uri; (uri = linkQueue.take()) != null; ) {
            if (!urlSet.add(uri)) continue;
            Thread current = Thread.currentThread();
            String threadName = current.getName();
            current.setName("Crawl: " + uri.toString());

            try {
                crawl(uri);
            } catch (IOException e) {
                System.out.println("URi Error: " + uri);
            } finally {
                current.setName(threadName);
            }
        }
    }

    // parallel bfs
    private void execute() throws Exception {
        if (fileCreated) {
            ExecutorService exec = Executors.newFixedThreadPool(threadCount);
            for (int i = 0; i < threadCount; i++) {
                exec.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            addNewUrls();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
            }

            exec.shutdown();
        }
    }

    // method to fetch links from the webpage
    public void crawl(URI url) throws Exception {
        AtomicInteger hostCount = new AtomicInteger();
        AtomicInteger prevCount = hostnames.putIfAbsent(url.getHost(), hostCount);

        if (prevCount != null) hostCount = prevCount;
        // if a host has been visited 150 times leave it
        if (hostCount.incrementAndGet() > 150) return;

        HttpGet request = new HttpGet(url);
        HttpResponse response = client.execute(request);

        if (response.getStatusLine().getStatusCode() != 200) {
            System.out.println("Error Code : " + response.getStatusLine().getStatusCode() + " Path: " +
                    url.toString());
            return;
        }

        System.out.println("Response Code : " + response.getStatusLine().getStatusCode() + " Path: " +
                url.toString());

        String contentType = response.getEntity().getContentType().getValue();
        // Content type format - "format;charset"
        String[] parts = contentType.split(";");
        String format = parts[0];

        if (!format.equalsIgnoreCase("text/html")) {
            System.out.println("Error bad format: " + contentType); // if not html return
            return;
        }

        // get the entity data
        String html = EntityUtils.toString(response.getEntity());

        // parse the html string
        org.jsoup.nodes.Document document = Jsoup.parse(html, url.toString());

        URIBuilder builder = new URIBuilder(url);
        URI baseLink = builder.build();

        StringBuilder sb = new StringBuilder(url.toString()).append("\t");
        for (org.jsoup.nodes.Element element : document.select("a[href]")) {
            String href = element.attr("href");
            URI childLink = null;
            try {
                childLink = baseLink.resolve(href);
            } catch (IllegalArgumentException e) {
                System.out.println("Bad child link");
            }
            if (childLink != null) {
                sb.append(1).append("\t");
                sb.append(childLink).append("\t");
                linkQueue.add(childLink);
            }
        }

        synchronized (bw) {
            //bw.write()
            //bw.newLine()
            bw.append(sb.toString());
            bw.append("\n");
        }

    }

    public static void main(String[] args) throws Exception {

        String[] pArgs = new GenericOptionsParser(new Configuration(),args).getRemainingArgs();
        if(pArgs.length != 3){
            System.out.println("Usage: <number of threads> <Starting Link> <out>");
            System.exit(2);
        }

        int thread = Integer.parseInt(pArgs[0]) ;
        RequestConfig defaultRequestConfig = RequestConfig.custom()
                .setCookieSpec(CookieSpecs.STANDARD)
                .build();
        CloseableHttpClient client = HttpClients.custom().
                setDefaultRequestConfig(defaultRequestConfig).
                build();


        // add resource directories
        String[] resourcePaths = {"/home/ashu/hadoop/etc/hadoop/core-site.xml",
                                  "/home/ashu/hadoop/etc/hadoop/hdfs-site.xml"};


        WebCrawler webCrawler = new WebCrawler(client, thread);
        webCrawler.createFile(pArgs[2],resourcePaths);
       // webCrawler.createFile("c:\\Users\\ashu\\file3.tsv");
        URI uri = new URI(pArgs[1]);
        webCrawler.linkQueue.add(uri);
        webCrawler.execute();
    }

}
