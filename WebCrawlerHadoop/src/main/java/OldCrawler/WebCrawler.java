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
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by ashu on 3/11/2017.
 */

public class WebCrawler {

    private final HttpClient client;
    private final LinkedBlockingDeque<URI> linkQueue = new LinkedBlockingDeque<>(10000); // a blocking list for URI's
    private String path;
    private BufferedWriter bw;
    // count the number of times a host has been visited
    private final ConcurrentHashMap<String, AtomicInteger> hostnames = new ConcurrentHashMap<String, AtomicInteger>();
    // keep unique uri's
    private final Set<URI> urlSet = Collections.synchronizedSet(new LinkedHashSet<URI>());
    private int threadCount;
    private boolean fileCreated = false;
    private String output;
    private String[] resourcePath;
    private LinkedList<URI> drainQueue;

    public WebCrawler(CloseableHttpClient client, int threadCount, String output, String[] resourcePath) throws Exception {
        this.client = client;
        this.threadCount = threadCount;
        this.output = output;
        this.path = output;
        this.resourcePath = resourcePath;
        drainQueue = new LinkedList<>();
        //  bw = FileWriters.HDFSFileWriter.get(output,resourcePath);
    }

    public boolean createFile(String output, String[] resourcePaths) {
        // this.path = path;
        File file = null;
        try {
            file = new File(path);
            if (!file.exists()) {
                file.createNewFile();
            }

            //bw = FileWriters.HDFSFileWriter.get(output,resourcePaths);
            //  bw = new BufferedWriter(new FileWriter(path, true));
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
        for (URI uri; (uri = linkQueue.poll(1, TimeUnit.SECONDS)) != null; ) {
            if (!urlSet.add(uri)) continue;
            Thread current = Thread.currentThread();
            String threadName = current.getName();
            current.setName("Crawl: " + uri.toString());

            try {
                crawl(uri);
            } catch (IOException e) {
                e.printStackTrace();
                // System.out.println("URi Error: " + uri);
            } finally {
                current.setName(threadName);
            }
        }
    }

    // parallel bfs
    private void execute() throws Exception {
        ExecutorService exec = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            exec.submit(new Runnable() {
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
        exec.awaitTermination(5, TimeUnit.SECONDS);
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


        String contentType = response.getEntity().getContentType().getValue();
        // Content type format - "format;charset"
        String[] parts = contentType.split(";");
        String format = parts[0];

        if (!format.equalsIgnoreCase("text/html")) {
            System.out.println("Error bad format: " + contentType); // if not html return
            return;
        }

        System.out.println("Response Code : " + response.getStatusLine().getStatusCode() + " Path: " +
                url.toString());

        // get the entity data
        String html = EntityUtils.toString(response.getEntity());

        // parse the html string
        org.jsoup.nodes.Document document = Jsoup.parse(html, url.toString());

        URIBuilder builder = new URIBuilder(url);
        URI baseLink = builder.build();

        bw = new BufferedWriter(new FileWriter(path, true));
        //  StringBuilder sb = new StringBuilder(url.toString()).append("\t");
        // sb.append(1).append("\t");
        bw.append(url.toString()).append("\t");
        synchronized (bw) {
            document.select("a[href]").size();
            for (org.jsoup.nodes.Element element : document.select("a[href]")) {
                String href = element.attr("href");
                URI childLink = null;
                try {
                    childLink = baseLink.resolve(href);
                } catch (IllegalArgumentException e) {
                    //  System.out.println("Bad child link");
                }
                if (childLink != null) {
                    bw.append(childLink.toString()).append("\t");
                    try {
                        linkQueue.add(childLink);
                    } catch (IllegalStateException e) {
                        linkQueue.drainTo(drainQueue, 2000); // sacrifice some links so that threads keeps block forever
                        drainQueue.clear();
                    }

                }
            }
            bw.newLine();
            //bw.write()
            //bw.newLine()
            System.out.println("Writing to file and queue size is: " + linkQueue.size());
        }
    }


    public static void main(String[] args) throws Exception {

      /*  String[] pArgs = new GenericOptionsParser(new Configuration(),args).getRemainingArgs();
        if(pArgs.length != 3){
            System.out.println("Usage: <number of threads> <Starting Link> <out>");
            System.exit(2);
        }*/

        // int thread = Integer.parseInt(pArgs[0]) ;
        RequestConfig defaultRequestConfig = RequestConfig.custom()
                .setCookieSpec(CookieSpecs.STANDARD)
                .build();


        CloseableHttpClient client = HttpClients.custom().
                setDefaultRequestConfig(defaultRequestConfig).
                build();


        // add resource directories
        String[] resourcePaths = {"/home/ashu/hadoop/etc/hadoop/core-site.xml",
                "/home/ashu/hadoop/etc/hadoop/hdfs-site.xml"};


        String output = "c:\\Users\\ashu\\file5.tsv";
        WebCrawler webCrawler = new WebCrawler(client, 100, output, resourcePaths);
        webCrawler.createFile(output, resourcePaths);
        // webCrawler.createFile();
        URI uri = new URI("http://www.youtube.com/");
        webCrawler.linkQueue.add(uri);
        webCrawler.execute();
    }

}
