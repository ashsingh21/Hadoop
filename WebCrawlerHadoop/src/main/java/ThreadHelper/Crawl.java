package ThreadHelper;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;

import java.net.URI;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by ashu on 3/13/2017.
 */
public class Crawl implements Callable<StringBuilder> {

    private URI uri;
    private HttpClient client;
    private LinkedBlockingDeque<URI> uriLinkedBlockingDeque;
    private ConcurrentHashMap<String,AtomicInteger> hostnames;

    public Crawl(URI uri, LinkedBlockingDeque<URI> uriLinkedBlockingDeque,
                 HttpClient client, ConcurrentHashMap<String,AtomicInteger> hostnames) {
        this.uri = uri;
        this.client = client;
        this.uriLinkedBlockingDeque = uriLinkedBlockingDeque;
        this.hostnames = hostnames;
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
                    url.toString());
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
                //  System.out.println("Bad child link");
            }
            if (childLink != null) {
                uriLinkedBlockingDeque.add(childLink);
                sb.append(childLink.toString()).append("\t");
            }
        }
        return sb;
    }
}
