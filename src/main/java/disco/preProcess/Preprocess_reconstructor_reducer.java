package disco.preProcess;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class Preprocess_reconstructor_reducer extends
		Reducer<IntWritable,IntWritable, IntWritable,IntWritable> {
	int sum;
	IntWritable ret = new IntWritable();
	@Override
	protected void reduce(
			IntWritable arg0,
			Iterable<IntWritable> arg1,
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		sum=0;
		for(IntWritable val : arg1){
			sum+=val.get();
		}
		ret.set(sum);
		context.write(arg0,ret);
	}

}
