import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by ashu on 3/11/2017.
 */

public class WebCrawler {

    private final HttpClient client;
    private final LinkedBlockingQueue<URI> linkQueue = new LinkedBlockingQueue<URI>(); // a blocking list for URI's
    private String path;
    private BufferedWriter bw;
    // count the number of times a host has been visited
    private final ConcurrentHashMap<String, AtomicInteger> hostnames = new ConcurrentHashMap<String, AtomicInteger>();
    // keep unique uri's
    private final Set<URI> urlSet = Collections.synchronizedSet(new LinkedHashSet<URI>());
    private int threadCount;
    private Object lock = new Object();

    public WebCrawler(HttpClient client, int threadCount) {
        this.client = client;
        this.threadCount = threadCount;
    }

    public boolean createFile(String path) {
        this.path = path;
        File file = null;
        try {
            file = new File(path);
            if (!file.exists()) {
                file.createNewFile();
                return true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
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
                System.out.print("URi: " + uri + " ");
                e.printStackTrace();
            } finally {
                current.setName(threadName);
            }
        }
    }

    // parllelise bfs
    private void parellize() throws Exception {
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

    // method to fetch links from the webpage
    public void crawl(URI url) throws Exception {
        AtomicInteger hostCount = new AtomicInteger();
        AtomicInteger prevCount = hostnames.putIfAbsent(url.getHost(), hostCount);

        if (prevCount != null) hostCount = prevCount;
        // if a host has been visited 150 times leave it
        if (hostCount.incrementAndGet() > 150) return;

        HttpGet request = new HttpGet(url);
        HttpResponse response = client.execute(request);

        System.out.println("Response Code : " + response.getStatusLine().getStatusCode() + " Path: " +
                url.toString());
        String contentType = response.getEntity().getContentType().getValue();


        if (response.getStatusLine().getStatusCode() != 200) {
            return;
        }

        // Content type format - "type;charset"
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


        bw = new BufferedWriter(new FileWriter(path, true));
        synchronized (bw) {
            StringBuilder sb = new StringBuilder(url.toString()).append(",");
            for (org.jsoup.nodes.Element element : document.select("a[href]")) {
                String href = element.attr("href");
                URI childLink = baseLink.resolve(href);
                sb.append(childLink).append(",");
                linkQueue.add(childLink);
            }
            bw.write(sb.toString());
            bw.newLine();
            bw.close();
        }

    }

    public static void main(String[] args) throws Exception {
        int thread = 20;

        HttpClient client = HttpClientBuilder.create().build();

        WebCrawler webCrawler = new WebCrawler(client, thread);
        webCrawler.createFile("c:\\Users\\ashu\\file.txt");
        URI uri = new URI("http://www.youtube.com");
        webCrawler.linkQueue.add(uri);
        webCrawler.parellize();
    }

}
