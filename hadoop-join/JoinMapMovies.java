import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// created by Ashutosh Singh


/*
* Dataset input format
* movie id, title, genres
*/

public class JoinMapMovies extends
	 Mapper<LongWritable, Text, CustomWritable, Text>{


        @Override
	 	public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException{
	 		
			String line = value.toString();
	 		String[] parts = line.trim().split(",");

	 		String movieId = parts[0]; 
	 		String title = parts[1];

            // tag 1 so that it comes first to the reducer
	 		ctx.write(new CustomWritable(movieId,1), 
	 			new Text(title));

  	}

}