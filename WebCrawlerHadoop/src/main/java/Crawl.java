import FileWriters.MultipleFileWriter;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;

import java.net.URI;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by ashu on 3/13/2017.
 */
public class Crawl implements Runnable {

    private URI uri;
    private HttpClient client;
    private LinkedBlockingDeque<URI> uriLinkedBlockingQeque;
    private ConcurrentHashMap<String, AtomicInteger> hostnames;
    private AtomicInteger atomicInteger;
    private int linksSize;
    private AtomicLong fileCounter;

    public Crawl(URI uri, LinkedBlockingDeque<URI> uriLinkedBlockingQeque,
                 HttpClient client, ConcurrentHashMap<String, AtomicInteger> hostnames,
                 AtomicInteger atomicInteger, int linksSize, AtomicLong fileCounter) {
        this.uri = uri;
        this.client = client;
        this.uriLinkedBlockingQeque = uriLinkedBlockingQeque;
        this.hostnames = hostnames;
        this.atomicInteger = atomicInteger;
        this.linksSize = linksSize;
        this.fileCounter = fileCounter;
    }

    @Override
    public void run() {
        try {
            crawl(uri);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // method to fetch links from the webpage
    public void crawl(URI url) throws Exception {
        AtomicInteger hostCount = new AtomicInteger();
        AtomicInteger prevCount = hostnames.putIfAbsent(url.getHost(), hostCount);

        if (prevCount != null) hostCount = prevCount;
        // if a host has been visited 150 times leave it
        if (hostCount.incrementAndGet() > 30000) return;

        HttpGet request = new HttpGet(url);
        HttpResponse response = client.execute(request);


        if (response.getStatusLine().getStatusCode() != 200) {
            System.out.println("Error Code : " + response.getStatusLine().getStatusCode() + " Path: " +
                    url.toString()); // not successful connection
            return;
        }

        if (response.getEntity().getContentType().getValue() == null) return; // most probably unknown host
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

        // start building the data for the output file
        StringBuilder sb = new StringBuilder(url.toString()).append("\t");
        sb.append(1).append("\t");
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
                if (atomicInteger.intValue() < linksSize) {
                    uriLinkedBlockingQeque.add(childLink);
                }
                sb.append(childLink.toString()).append("\t");
            }
        }

        // write the urls to the file
        new MultipleFileWriter().writeToFile(sb, fileCounter.incrementAndGet());
        //  HDFSFileWriter.getInstance().writeToFile(sb);
    }
}
