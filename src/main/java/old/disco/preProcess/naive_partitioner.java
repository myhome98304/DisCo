package old.disco.preProcess;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class naive_partitioner extends Partitioner<IntWritable, LongWritable> {

	@Override
	public int getPartition(IntWritable key, LongWritable val, int numReduceTasks) {
		// TODO Auto-generated method stub
		
		return (int)(Math.random()*numReduceTasks);        
	}

}
