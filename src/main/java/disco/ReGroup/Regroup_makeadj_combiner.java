package disco.ReGroup;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Regroup_makeadj_combiner extends
		Reducer<IntWritable, Text, IntWritable, Text> {
	Text value = new Text();
	String job;

	int k, l;

	int[] value_change;
	
	int i;
	
	StringTokenizer st1, st2;
	HashMap<String, Integer> split;

	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		k = conf.getInt("k", 0);
		l = conf.getInt("l", 0);
		job = conf.get("job", "");
	}

	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		if (job.equals("r"))
			value_change = new int[k];
		else
			value_change = new int[l];

		for (Text line : values) {
			if (line.toString().startsWith("y")) {
				context.write(key, line);
				continue;
			}
			st1 = new StringTokenizer(line.toString(), " ");
			i = 0;
			while (st1.hasMoreTokens()) {
				value_change[i++] += Integer.parseInt(st1.nextToken());
			}

		}
		value.set(arrToString(value_change));
		context.write(key, value);

	}

	private static String arrToString(int[] arr) {
		String ret = "";
		for (int d : arr) {
			ret += d + " ";
		}
		return ret;
	}

}
