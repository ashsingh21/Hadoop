import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.client.utils.URIUtils;
import org.jsoup.Jsoup;

import javax.lang.model.element.Element;
import java.net.URI;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by ashu on 3/11/2017.
 */

public class WebCrawler {

    private final HttpClient client;
    private final LinkedBlockingQueue<URIBuilder> linkQueue = new LinkedBlockingQueue<URIBuilder>();
    private final ConcurrentHashMap<String, AtomicInteger> hostnames = new ConcurrentHashMap<String, AtomicInteger>();
    private final Set<URIBuilder> urlSet = Collections.synchronizedSet(new LinkedHashSet<URIBuilder>());

    public WebCrawler(HttpClient client){
        this.client = client;
    }

    public void getURL(URIBuilder url) throws Exception{
        AtomicInteger hostCount = new AtomicInteger();
        AtomicInteger prevCount = hostnames.putIfAbsent(url.getHost(),hostCount);

        if(prevCount != null) hostCount = prevCount;
        if(hostCount.incrementAndGet() > 100) return;

        HttpGet request = new HttpGet(url.getPath());
        HttpResponse response = client.execute(request);

        System.out.println("Response Code : " + response.getStatusLine().getStatusCode() +
                            url.getPath());
        String contentType = response.getEntity().getContentType().getValue();
        if(response.getStatusLine().getStatusCode() != 200 || contentType == null){
            return;
        }

        String[] parts = contentType.split(";");
        String format = parts[0];
        String charset = parts[1];

        if(!format.equalsIgnoreCase("text/html")){
            System.out.println("Error bad format: " + contentType);
            return;
        }

        org.jsoup.nodes.Document document = Jsoup.parse(response.getEntity().getContent(),charset,url.getPath());

        for(org.jsoup.nodes.Element element: document.select("a[href]")){
            String href = element.attr("href");

        }


    }


}
