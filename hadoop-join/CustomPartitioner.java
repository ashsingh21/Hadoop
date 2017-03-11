
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitioner extends Partitioner<CustomWritable, Text>{

	@Override
	public int getPartition(CustomWritable key, Text value, int numberReduceTasks){
		return (key.getJoinKey().hashCode() % numberReduceTasks); // partition based on join key
	}
}