package disco.preProcess;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Preprocess_sum_mapper extends
		Mapper<LongWritable, Text, IntWritable, LongWritable> {
	int num_machine;
	LongWritable value = new LongWritable();
	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		num_machine=conf.getInt("num_machine", 0);
	}
	@Override
	public void map(LongWritable arg0, Text line, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		value.set(Long.parseLong(line.toString().split("\t")[2]));
		context.write(new IntWritable((int)Math.random()*num_machine), value);
	}
}