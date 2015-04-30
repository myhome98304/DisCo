package old.disco.preProcess;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Preprocess_reducer extends
		Reducer<IntWritable, Text, IntWritable, Text> {
	Text ret_text = new Text();
	MultipleOutputs<IntWritable, Text> mos;
	String job;
	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Iterator<Text> val = values.iterator();
		String ret = "";
		while(val.hasNext()){
			ret+=val.next().toString()+" ";
		}
		ret_text.set(ret);
		mos.write("adj", key, ret_text, "adj/adj");
		ret_text.set(0+"");
		mos.write(job+"assign", key, ret_text, "assign/"+job+"assign");
	}

	@Override
	protected void cleanup(
			Reducer<IntWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		mos.close();
	}

	@Override
	protected void setup(
			Reducer<IntWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		mos = new MultipleOutputs<IntWritable, Text>(context);
		job = context.getConfiguration().get("job","");
	}

}