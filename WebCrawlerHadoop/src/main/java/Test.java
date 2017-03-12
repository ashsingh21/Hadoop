import org.apache.hadoop.fs.FileSystem;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;
import org.apache.hadoop.conf.Configuration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import static org.apache.http.HttpHeaders.USER_AGENT;

/**
 * Created by ashu on 3/11/2017.
 */
public class Test {

    public static void main(String... args) throws Exception{

        String url = "http://www.youtube.com/";

        HttpClient client = HttpClientBuilder.create().build();
        HttpGet request = new HttpGet(url);

        // add request header
        request.addHeader("User-Agent", USER_AGENT);
        HttpResponse response = null;
        try {
            response = client.execute(request);
        }catch (IOException e){
            e.printStackTrace();
        }


        BufferedReader rd = null;
        /*try {
            rd = new BufferedReader(
                    new InputStreamReader(response.getEntity().getContent()));
        }catch (IOException e){
                e.printStackTrace();
        }*/

/*
        StringBuilder result = new StringBuilder();
        String line = "";
        while ((line = rd.readLine()) != null) {
            result.append(line);
        }*/

        String con = response.getEntity().getContentType().getValue();
        System.out.println(con);

        String[] parts = con.split(";");
        System.out.println(con == null);



        String a = EntityUtils.toString(response.getEntity());
        System.out.print(a);
        org.jsoup.nodes.Document document = Jsoup.parse(a,"http://www.youtube.com");

        Configuration conf = new Configuration();

        URIBuilder builder = new URIBuilder("http://www.youtube.com");
        URI link = builder.build();
        System.out.print(link.toString());
        for (org.jsoup.nodes.Element element : document.select("a[href]")) {
            String href = element.attr("href");
            URI f = link.resolve(href);
            System.out.println(f.getHost());

        }
    }
}
