package disco.ReGroup;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class naive_partitioner_reg extends Partitioner<IntWritable, Text> {

	@Override
	public int getPartition(IntWritable key, Text Text, int numReduceTasks) {
		// TODO Auto-generated method stub
		
		return (int)(Math.random()*numReduceTasks);        
	}

}
