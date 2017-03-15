package Pagerank;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by ashu on 3/15/2017.
 */
public class DescendingComparator extends WritableComparator {

    public DescendingComparator(){
        super(DoubleWritable.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2){
        DoubleWritable d1 = (DoubleWritable) w1;
        DoubleWritable d2 = (DoubleWritable) w2;

        return d2.compareTo(d1);
    }
}
