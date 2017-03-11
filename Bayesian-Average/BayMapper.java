import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//  Created by Ashutosh Singh

// Bayesian average of ratings from data joined using hadoop-join

public class BayMapper extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	public void map(LongWritable key, Text value, Context ctx)
	throws IOException,InterruptedException{

		String line = value.toString();
		String[] parts = line.trim().split(",");


		// K - movie id, V - "Rating,Title"
		ctx.write(new Text(parts[0]), new Text(parts[2] + "," + parts[3]));
	}
}