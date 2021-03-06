package disco.preProcess;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Preprocess_c_mapper extends
		Mapper<LongWritable, Text, IntWritable, Text> {
	IntWritable key=new IntWritable();
	Text value=new Text();
	@Override
	public void map(LongWritable arg0, Text line, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] s = line.toString().split("\t");
		StringTokenizer st = new StringTokenizer(s[1]," ");
		value.set(s[0]);
		while(st.hasMoreTokens()){
			key.set(Integer.parseInt(st.nextToken()));
			context.write(key, value);
		}
	}
}