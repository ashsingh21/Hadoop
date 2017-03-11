import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import java.lang.StringBuilder;
import org.apache.hadoop.mapreduce.Reducer;

//  Created by Ashutosh Singh

// Aggregrates all the ratings for a movie id and calculates it bayesian average

// output format - movie id, bayesian average, Title

public class BayReducer extends Reducer<Text, Text, Text, Text>{

	@Override
	public void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException{

        long count = 1;
        double bayAvg = 0;

        // these values were estimated from analysing the sample of data 

        double m = 3.4; // value towards which the ratings will be adjusted 

        int c= 70; // minimum number of reviews to get away from m
        
        Iterator<Text> itr = values.iterator();

        Text val = itr.next();
        String[] parts = val.toString().split(",");
        String title = parts[1];

        double rating = 0;
         
        try{
            rating = Double.parseDouble(parts[0]);

        } catch(NumberFormatException e){
                rating = 0;
        }


        while (itr.hasNext()) {
        	val = itr.next();
        	parts = val.toString().split(",");

            double rate = 0;

            try{
                rate  = Double.parseDouble(parts[0]);

            } catch(NumberFormatException e){
                rate = 0;
            }   

        	rating += rate;
        	count++;
        }

        // Bayesian Average
        bayAvg =  ((c * m) + rating) / c + count;

        StringBuilder sb = new StringBuilder();
        sb.append(bayAvg).append(",").append(title);

        ctx.write(key, new Text(sb.toString()));

	}
}