package Pagerank;

import FileWriters.Conf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by ashu on 3/12/2017.
 */
public class PageRank {

    // input format tab sepertaed :  <Link> <Rank> <Outgoing Links>
    public static class ValueMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {

            String line = value.toString();
            String[] parts = line.trim().split("\\t+");
            String link = parts[0];
            double rank = 0.2;
            try {
                 rank = Double.parseDouble(parts[1]);
            } catch (NumberFormatException e){
                System.out.print("Not a valid number");
            }

            StringBuilder sb = new StringBuilder("|");
            if (parts.length > 2) {
                int totalOutlinks = parts.length - 2;
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

    public static class ValueReducer extends Reducer<Text, Text, Text, Text> {
        public static final float DMP = 0.85f;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            String links = "";
            Iterator<Text> itr = values.iterator();
            double calculatedRank = 0;

            while (itr.hasNext()) {
                String value = itr.next().toString();
                // if original links store them in links
                if (value.startsWith("|")) {
                    links = "\t" + value.substring(1);
                    continue;
                }

                String[] parts = value.split(",");
                double rank = 0.2;
                double totalOutLinks = 1;

                try {
                    rank = Double.valueOf(parts[1]);
                    totalOutLinks = Double.valueOf(parts[2]);
                }catch (NumberFormatException e){
                    System.out.println("Invalid number");
                }
                calculatedRank += (rank / totalOutLinks);
            }

            calculatedRank = DMP * calculatedRank + (1 - DMP);

            ctx.write(new Text(key), new Text("\t" + calculatedRank + links));

        }
    }

    // structure the output of Value Reducer
    public static class StructureMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

        @Override
        public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\\t");

            String link = parts[0];
            double rank = 0.2;
            try {
                rank = Double.parseDouble(parts[1]);
            }catch (NumberFormatException e){
                System.out.print("Not a valid number");
            }


            ctx.write(new DoubleWritable(rank), new Text(link));

        }

    }


    public void startPageRank(Path inputPath, Path outputPath) throws Exception {
        Configuration conf = Conf.getConf();

        Job job = Job.getInstance(conf, "Page Rank");

        job.setJarByClass(PageRank.class);
        job.setMapperClass(ValueMapper.class);
        job.setReducerClass(ValueReducer.class);

        // good practice to set all the key value classes even though they are same
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);

    }

    public void structureOutput(Path inputPath, Path outpath) throws Exception {
        Configuration conf = Conf.getConf();

        Job job = Job.getInstance(conf, "Structure output");

        job.setJarByClass(PageRank.class);
        job.setMapperClass(StructureMapper.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outpath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }


    public void run(int runs) throws Exception {
        String output = "output";

        FileSystem fs = null;
        try {
            fs = FileSystem.get(Conf.getConf());
        } catch (IOException e) {
            System.out.println("Can't create File System: " + e);
        }

        Path workingDir = fs.getHomeDirectory();
        Path out = new Path("/WebCrawler/iter0/" + output + ".tsv");

        Path completePath = Path.mergePaths(workingDir, out);

        int n;
        for (n = 0; n < runs; n++) {
            Path inputPath = Path.mergePaths(workingDir, new Path("/WebCrawler/iter" + n + "/"));
            Path outputPath = Path.mergePaths(workingDir, new Path("/WebCrawler/iter" + (n + 1) + "/"));
            startPageRank(inputPath, outputPath);
        }

        structureOutput(Path.mergePaths(workingDir, new Path("/WebCrawler/iter" + n + "/")),
                Path.mergePaths(workingDir, new Path("/WebCrawler/PageRank/")));

    }

}
