package Pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by ashu on 3/12/2017.
 */
public class PageRank {

    // input format tab sepertaed :  <Link> <Rank> <Outgoing Links>
    public class ValueMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {

            String line = value.toString();
            String[] parts = line.trim().split("\t");
            String link = parts[0];
            double rank = Double.parseDouble(parts[1]);
            int totalOutlinks = 0;
            StringBuilder sb = new StringBuilder("|");
            if (parts.length > 2) {
                totalOutlinks = parts.length - 2;
                for (int i = 3; i < parts.length; i++) {
                    // output format "," seperated : key: <outgoing link>  value: <link> <rank> <total outgoing links>
                    ctx.write(new Text(parts[i]), new Text(link + "," + rank + "," + totalOutlinks));
                    sb.append(parts[i]).append("\t");
                }

                // send back the original links
                ctx.write(new Text(link), new Text(sb.toString()));
            }
        }

    }

    public class ValueReducer extends Reducer<Text, Text, Text, Text> {
        public static final float DMP = 0.85f;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            String links = "";
            Iterator<Text> itr = values.iterator();
            double calculatedRank = 0;


            while (itr.hasNext()) {
                String value = itr.next().toString();
                // if original links store them in links
                if(value.startsWith("|")){
                    links = "\t" + value.substring(1);
                    continue;
                }

                String[] parts = value.split(",");
                double rank = Double.valueOf(parts[1]);
                int totalOutLinks = Integer.valueOf(parts[2]);

                calculatedRank += (rank/totalOutLinks);
            }

            calculatedRank = DMP * calculatedRank + (1 - DMP);

            ctx.write(new Text(key), new Text("\t" + calculatedRank + links));

        }
    }


    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Page Rank");


        job.setJarByClass(PageRank.class);
        job.setMapperClass(ValueMapper.class);
        job.setReducerClass(ValueReducer.class);
        
        // good practice to set all the key value classes even though they are same
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

    }

}
