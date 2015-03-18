package disco.preProcess;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Preprocess_reducer extends Reducer<Text, Text, Text, Text> {
	Text value = new Text();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Iterator<Text> val = values.iterator();
		String ret = "";

		while (val.hasNext())
			ret += val.next().toString() + " ";
		value.set(ret);
		context.write(key, value);
	}

}