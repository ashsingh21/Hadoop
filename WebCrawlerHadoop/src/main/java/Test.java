import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.util.EntityUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import static org.apache.http.HttpHeaders.USER_AGENT;

/**
 * Created by ashu on 3/11/2017.
 */
public class Test {

    public static void main(String... args) throws IOException{

        String url = "http://www.google.com/search?q=httpClient";

        HttpClient client = HttpClientBuilder.create().build();
        HttpGet request = new HttpGet(url);

        // add request header
       // request.addHeader("User-Agent", USER_AGENT);
        HttpResponse response = null;
        try {
            response = client.execute(request);
        }catch (IOException e){
            e.printStackTrace();
        }


        String val = response.getEntity().getContentType().getValue();
        String[] values = val.split(";");
        System.out.println(values[0] + values[1]);
        System.out.print(val);
        BufferedReader rd = null;
        try {
            rd = new BufferedReader(
                    new InputStreamReader(response.getEntity().getContent()));
        }catch (IOException e){
                e.printStackTrace();
        }


        StringBuilder result = new StringBuilder();
        String line = "";
        while ((line = rd.readLine()) != null) {
            result.append(line);
        }

        System.out.print(result.toString());

    }
}
