package old.disco.preProcess;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class Preprocess_sum_reducer extends
		Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {
	@Override
	public void reduce(IntWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
		int ret=0;
		Iterator<LongWritable> val = values.iterator();
		while(val.hasNext()){
			ret+=val.next().get();
		}
		context.write(key,new LongWritable(ret));
	}
}
