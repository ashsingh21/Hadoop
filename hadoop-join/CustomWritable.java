
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

// created by Ashutosh Singh

public class CustomWritable implements WritableComparable<CustomWritable>{

	private String joinKey = ""; // movie id
	private int tag; // tag identify the dataset


	public CustomWritable(){
		
	}

	public CustomWritable(String joinKey, int tag){
		this.joinKey = joinKey;
		this.tag = tag;
	}


	// if same keys, sort based on tag (secondary sorting)
	@Override
	public int compareTo(CustomWritable key){
		int comp = this.joinKey.compareTo(key.getJoinKey());
        
		if(comp == 0){
			comp = Integer.compare(this.tag, key.getTag());
		}

		return comp;
	}

    
    // deserialise this custom writable
	@Override
	public void readFields(DataInput in) throws IOException{
		joinKey = WritableUtils.readString(in);
		tag = WritableUtils.readVInt(in);
	}

	// serialize this custom writable
	@Override
	public void write(DataOutput out) throws IOException{
		WritableUtils.writeString(out, joinKey);
		WritableUtils.writeVInt(out,tag);
	}

	// setters and getters
	public String getJoinKey(){
		return joinKey;
	}

	public void set(String joinKey){
		this.joinKey = joinKey;
	}

	public void set(int tag){
		this.tag = tag;
	}

	public int getTag(){
		return tag;
	}


} 