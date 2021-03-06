package disco.ReGroup;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class RG_output_mapper extends
		Mapper<LongWritable, Text, IntWritable, Text> {

	Text k = new Text();
	IntWritable k1 =new IntWritable();
	

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		StringTokenizer st1 = new StringTokenizer(value.toString(),"\t");
		String cluster = st1.nextToken();
		
		k.set(cluster);
		context.write(new IntWritable(Integer.parseInt(cluster)), new Text(st1.nextToken()+ "\t" + st1.nextToken()));
		
		StringTokenizer st2 = new StringTokenizer(st1.nextToken());
		
		while(st2.hasMoreTokens()){
			k1.set(-1-Integer.parseInt(st2.nextToken()));
			context.write(k1, k);
		}
			
	}
}
