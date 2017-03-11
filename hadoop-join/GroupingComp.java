import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

// created by Ashutosh Singh

public class GroupingComp extends WritableComparator{

	protected GroupingComp(){
		super(CustomWritable.class, true);
	}

	// grouping using join key (all keys with same joinKey go to same reducer)
	@Override
	public int compare(WritableComparable w1, WritableComparable w2){

		CustomWritable key1 = (CustomWritable) w1;
		CustomWritable key2 = (CustomWritable) w2;

		return key1.getJoinKey().compareTo(key2.getJoinKey());
	}
}