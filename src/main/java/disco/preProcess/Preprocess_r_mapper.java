package disco.preProcess;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Preprocess_r_mapper extends
		Mapper<LongWritable, Text, IntWritable, Text> {

	@Override
	public void map(LongWritable arg0, Text line, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] s = line.toString().split(" ");

		int src = Integer.parseInt(s[0]);
		int dst = Integer.parseInt(s[1]);

		context.write(new IntWritable(src),new Text(dst+""));

	}
}