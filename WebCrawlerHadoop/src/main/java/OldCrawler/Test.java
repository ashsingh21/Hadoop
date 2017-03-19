package OldCrawler;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ashu on 3/11/2017.
 */
public class Test {

    public static void main(String... args) throws Exception {

        combine(4,2);

        String url = "http://www.youtube.com/";

        String a = "https://el.wikipedia.org/wiki/\t1\t\thttps://el.wikipedia.org/wiki/#mw-head\t";
        StringBuilder sb = new StringBuilder("|");
        sb.append(a);
        if(sb.toString().startsWith("|")) System.out.print(sb.toString().substring(1));
       // System.out.println(Integer.parseInt(parts[1]));

     /*   HttpClient client = HttpClientBuilder.create().build();
        HttpGet request = new HttpGet(url);

        // add request header
        request.addHeader("User-Agent", USER_AGENT);
        HttpResponse response = null;
        try {
            response = client.execute(request);
        } catch (IOException e) {
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

     /*   String con = response.getEntity().getContentType().getValue();
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
    }*/
    }

    public static List<List<Integer>> combine(int n, int k) {
        List<List<Integer>> combs = new ArrayList<List<Integer>>();
        combine(combs, new ArrayList<Integer>(), 1, n, k);
        return combs;
    }
    public static void combine(List<List<Integer>> combs, List<Integer> comb, int start, int n, int k) {
        if(k==0) {
            combs.add(new ArrayList<Integer>(comb));
            System.out.println("List: " + start + " "  + combs);
            return;
        }
        for(int i=start;i<=n;i++) {
            comb.add(i);
            System.out.println(" i added " + i);
            System.out.println("Comb: " + comb);
            combine(combs, comb, i+1, n, k-1);
            comb.remove(comb.size() - 1);
        }
    }
}
