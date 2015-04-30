package disco.IncCluster;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class IncCluster_calc_mapper extends
		Mapper<IntWritable, Text, IntWritable, Text> {

	@Override
	protected void map(IntWritable key, Text value,
			Mapper<IntWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		context.write(key,value);
	}

}
