package disco.preProcess;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Preprocess_combiner extends Reducer<IntWritable, Text, IntWritable, Text> {
	Text value = new Text();
	int num;
	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Iterator<Text> val = values.iterator();
		String ret = "";
		num=0; 
		while (val.hasNext()){
			ret += val.next().toString() + " ";
			num++;
		}
		System.out.println(key+" "+num+ " "+ret);
		value.set(num+"\t"+ret);
		context.write(key, value);
	}

}