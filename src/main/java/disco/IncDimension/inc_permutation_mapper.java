package disco.IncDimension;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class inc_permutation_mapper extends
		Mapper<LongWritable, Text, IntWritable, Text> {
	int k,l;
	
	@Override
	public void run(
			Mapper<LongWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = context.getConfiguration();
		k = conf.getInt("k",0);
		l = conf.getInt("l",0);
		
	}

	@Override
	protected void setup(
			Mapper<LongWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

}
