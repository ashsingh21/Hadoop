import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;
 

// created by Ashutosh Singh

public class BayesianAverage {
	public static void main(String[] args) throws Exception {
       Configuration conf = new Configuration();
       
       
     String[] programArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
     if (programArgs.length != 2) {
        System.err.println("Usage: BayesianAverage <in>  <out>");
        System.exit(2);
     }
     Job job = Job.getInstance(conf, "BayesianAverage");
    
     job.setJarByClass(BayesianAverage.class);
     job.setMapperClass(BayMapper.class);
     job.setReducerClass(BayReducer.class);

     FileInputFormat.addInputPath(job, new Path(programArgs[0]));


     job.setOutputKeyClass(Text.class);
     job.setOutputValueClass(Text.class);
     
     FileOutputFormat.setOutputPath(job, new Path(programArgs[1]));
    
     System.exit(job.waitForCompletion(true) ? 0 : 1);
	}	
}
