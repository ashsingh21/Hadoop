import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;

// created by Ashutosh Singh

public class Reviews {
	public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
       
     String[] programArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
     if (programArgs.length != 3) {
        System.err.println("Usage: Reviews <in> <in> <out>");
        System.exit(2);
     }

     Job job = Job.getInstance(conf, "Reviews Join");
     job.setJarByClass(Reviews.class);
     
     // define mapper for both datasets
     MultipleInputs.addInputPath(job, new Path(programArgs[0]), TextInputFormat.class, JoinMapMovies.class);
     MultipleInputs.addInputPath(job, new Path(programArgs[1]), TextInputFormat.class, JoinMapRatings.class);

     // set reducer
     job.setReducerClass(JoinReducer.class);

     // define partitioner and grouping comparator
     job.setPartitionerClass(CustomPartitioner.class);
     job.setGroupingComparatorClass(GroupingComp.class);

     // set <K,V> output formats for mappers and reducer
     job.setMapOutputKeyClass(CustomWritable.class);
     job.setMapOutputValueClass(Text.class);
     job.setOutputKeyClass(NullWritable.class);
     job.setOutputValueClass(Text.class);
     
     FileOutputFormat.setOutputPath(job, new Path(programArgs[2]));
    
     System.exit(job.waitForCompletion(true) ? 0 : 1);
	}	
}
