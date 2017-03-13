import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;

import java.net.URI;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by ashu on 3/13/2017.
 */
public class Crawl implements Callable<StringBuilder> {

    private URI uri;
    private HttpClient client;
    private LinkedBlockingQueue<URI> uriLinkedBlockingQeque;
    private ConcurrentHashMap<String,AtomicInteger> hostnames;
    private AtomicInteger atomicInteger;
    private int iterations;

    public Crawl(URI uri, LinkedBlockingQueue<URI> uriLinkedBlockingQeque,
                 HttpClient client, ConcurrentHashMap<String,AtomicInteger> hostnames,AtomicInteger atomicInteger, int iterations) {
        this.uri = uri;
        this.client = client;
        this.uriLinkedBlockingQeque = uriLinkedBlockingQeque;
        this.hostnames = hostnames;
        this.atomicInteger = atomicInteger;
        this.iterations = iterations;
    }

    @Override
    public StringBuilder call() throws Exception {
        return crawl(uri);
    }

    // method to fetch links from the webpage
    public StringBuilder crawl(URI url) throws Exception {
        AtomicInteger hostCount = new AtomicInteger();
        AtomicInteger prevCount = hostnames.putIfAbsent(url.getHost(), hostCount);

        if (prevCount != null) hostCount = prevCount;
        // if a host has been visited 150 times leave it
        if (hostCount.incrementAndGet() > 150) return null;

        HttpGet request = new HttpGet(url);
        HttpResponse response = client.execute(request);


        if (response.getStatusLine().getStatusCode() != 200) {
            System.out.println("Error Code : " + response.getStatusLine().getStatusCode() + " Path: " +
                    url.toString()); // not successful connection
            return null;
        }

        if(response.getEntity().getContentType().getValue() == null) return null; // most probably unknown host
        String contentType = response.getEntity().getContentType().getValue();

        // Content type format - "format;charset"
        String[] parts = contentType.split(";");
        String format = parts[0];

        if (!format.equalsIgnoreCase("text/html")) {
            System.out.println("Error bad format: " + contentType); // if not html return
            return null;
        }

        System.out.println("Response Code : " + response.getStatusLine().getStatusCode() + " Path: " +
                url.toString());

        // get the entity data
        String html = EntityUtils.toString(response.getEntity());

        // parse the html string
        org.jsoup.nodes.Document document = Jsoup.parse(html, url.toString());

        URIBuilder builder = new URIBuilder(url);
        URI baseLink = builder.build();


        StringBuilder sb = new StringBuilder(url.toString()).append("\t");
        sb.append(1).append("\t");
        int i = 0;
        document.select("a[href]").size();
        for (org.jsoup.nodes.Element element : document.select("a[href]")) {
            String href = element.attr("href");
            URI childLink = null;
            try {
                childLink = baseLink.resolve(href);
            } catch (IllegalArgumentException e) {
                 System.out.println("cant resolve child link");
            }
            if (childLink != null) {
                if(atomicInteger.get() <= iterations) {
                    uriLinkedBlockingQeque.add(childLink);
                }
                sb.append(childLink.toString()).append("\t");
            }
        }
        return sb;
    }
}
