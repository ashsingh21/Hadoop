import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import java.lang.StringBuilder;
import org.apache.hadoop.mapreduce.Reducer;

// created by Ashutosh Singh

/*
*  output format movie id, user id, rating , title
*
*/

public class JoinReducer extends 
	Reducer<CustomWritable,Text,NullWritable,Text> {

	@Override
	public void reduce(CustomWritable key, Iterable<Text> values, Context ctx)
	throws IOException, InterruptedException{

    StringBuilder sb = new StringBuilder();
    String title = "";
    
    Iterator<Text> itr = values.iterator();
    // since the key and value of dataset with tag 1 will always come first, 
    // we will always get the <K,V> of movie id, title first
    title = itr.next().toString();

    while(itr.hasNext()){

      String[] parts = itr.next().toString().split(",");
      sb.append(parts[0]).append(","); // user id
      sb.append(parts[1]).append(","); // rating
      sb.append(title);
      
      ctx.write(NullWritable.get(), new Text(key.getJoinKey() + "," + sb.toString()));

    }

  }
}