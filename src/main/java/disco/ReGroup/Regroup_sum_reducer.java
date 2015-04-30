package disco.ReGroup;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Regroup_sum_reducer extends
		Reducer<IntWritable, Text, IntWritable, Text> {
	Text value = new Text();
	String job;

	int k, l;
	int num_machine;
	int[] value_change;
	IntWritable red_key = new IntWritable();
	int num, i;
	int index;
	StringTokenizer st1, st2;
	String cluster, adj;
	String[] calc;
	boolean temp;

	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		k = conf.getInt("k", 0);
		l = conf.getInt("l", 0);
		job = conf.get("job", "");
		num_machine = conf.getInt("num_machine", 1);
	}

	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		if (job.equals("r"))
			value_change = new int[l];
		else
			value_change = new int[k];

		index = key.get();
		index = (index - (index % num_machine)) / num_machine;
		num = 0;
		for (Text line : values) {
			i = 0;
			st1 = new StringTokenizer(line.toString(), "\t");
			num += Integer.parseInt(st1.nextToken());
			st2 = new StringTokenizer(st1.nextToken(), " ");
			while (st2.hasMoreTokens())
				value_change[i++] += Long.parseLong(st2.nextToken());
		}
		value.set(num + "\t" + arrToString(value_change));
		key.set(index);
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