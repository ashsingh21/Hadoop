import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// created by Ashutosh Singh
/*
* Dataset input format
* user id, movie id, rating, timestamp
*/

public class JoinMapRatings extends
	 Mapper<LongWritable, Text, CustomWritable, Text>{

	 	@Override
	 	public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException{
	 	String line = value.toString();
	 		String[] parts = line.trim().split(",");

	 		String userId = parts[0];
	 		String movieId = parts[1];
	 		String rating =  parts[2];

	 		// tag 2
	 		ctx.write(new CustomWritable(movieId,2), 
	 			new Text(userId +  "," + rating));
	}

}