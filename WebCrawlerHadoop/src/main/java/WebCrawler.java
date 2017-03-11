import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.client.utils.URIUtils;
import org.jsoup.Jsoup;

import javax.lang.model.element.Element;
import java.io.IOException;
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

    // count the number of times a host has been visited
    private final ConcurrentHashMap<String, AtomicInteger> hostnames = new ConcurrentHashMap<String, AtomicInteger>();
    // keep unique uri's
    private final Set<URI> urlSet = Collections.synchronizedSet(new LinkedHashSet<URI>());
    private int threadCount;

    public WebCrawler(HttpClient client, int threadCount) {
        this.client = client;
        this.threadCount = threadCount;
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

    // paraleellise bfs
    private void parellize() {
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

        System.out.println("Response Code : " + response.getStatusLine().getStatusCode() +
                url.getPath());
        String contentType = response.getEntity().getContentType().getValue();
        if (response.getStatusLine().getStatusCode() != 200 || contentType == null) {
            return;
        }

        // Content type format - "type;charset"
        String[] parts = contentType.split(";");
        String format = parts[0];
        String charset = parts[1];

        if (!format.equalsIgnoreCase("text/html")) {
            System.out.println("Error bad format: " + contentType); // if not html return
            return;
        }

        org.jsoup.nodes.Document document = Jsoup.parse(response.getEntity().getContent(), charset, url.getPath());

        URIBuilder builder = new URIBuilder(url);

        for (org.jsoup.nodes.Element element : document.select("a[href]")) {
            String href = element.attr("href");
            URI link = builder.build().resolve(href);
            linkQueue.add(link);
        }

    }

    public static void main(String[] args){
        
    }

}
