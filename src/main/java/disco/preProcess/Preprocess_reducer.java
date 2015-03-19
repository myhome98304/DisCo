package disco.preProcess;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Preprocess_reducer extends Reducer<IntWritable, Text, IntWritable, Text> {
	Text value = new Text();
	Text number = new Text();
	StringTokenizer st;
	MultipleOutputs<IntWritable, Text> mos;
	int num;

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		mos.close();
	}
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		mos = new MultipleOutputs<IntWritable, Text>(context);
	}
	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		String ret = "";
		num=0;
		
		for(Text line :values){
			st = new StringTokenizer(line.toString(),"\t");
			num += Integer.parseInt(st.nextToken());
			ret += st.nextToken() + " ";
		}
		
		number.set(0+"\t"+num);
		value.set(ret);
		
		mos.write("adj", key, value,"adj/adj");
		mos.write("nonzero", key, number,"nonzero/nonzero");
	}

}