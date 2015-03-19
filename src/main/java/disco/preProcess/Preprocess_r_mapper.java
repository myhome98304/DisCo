package disco.preProcess;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Preprocess_r_mapper extends
		Mapper<LongWritable, Text, IntWritable, Text> {
	IntWritable key =new IntWritable();
	Text value = new Text();
	int src;
	StringTokenizer st;
	@Override
	public void map(LongWritable arg0, Text line, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		try{
			st = new StringTokenizer(line.toString(),"\t ");
			src = Integer.parseInt(st.nextToken());
			key.set(src);
			value.set(st.nextToken());
			context.write(key,value);	
		}
		catch(Exception e){
			return;
		}
		

	}
}