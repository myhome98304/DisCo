package disco.ReGroup;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Regroup_makeadj_reducer extends
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
	MultipleOutputs<IntWritable, Text> mos;
	boolean temp;

	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		k = conf.getInt("k", 0);
		l = conf.getInt("l", 0);
		job = conf.get("job", "");
		num_machine = conf.getInt("num_machine", 1);
		mos = new MultipleOutputs<IntWritable, Text>(context);
	}

	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		if (job.equals("r")) {
			value_change = new int[k];
		} else
			value_change = new int[l];
		temp = false;

		for (Text line : values) {

			if (!temp) {
				if (line.toString().startsWith("y")) {
					st1 = new StringTokenizer(line.toString(), "\t");
					cluster = st1.nextToken().substring(1);
					adj = st1.nextToken();
					temp = true;
					continue;
				}
			}

			i = 0;
			st1 = new StringTokenizer(line.toString(), " ");
			while (st1.hasMoreTokens()) {
				value_change[i++] += Integer.parseInt(st1.nextToken());
			}

		}

		value.set((job.equals("r") ? "c" : "r") + "\t" + cluster + "\t"
				+ arrToString(value_change) + "\t" + adj);

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
