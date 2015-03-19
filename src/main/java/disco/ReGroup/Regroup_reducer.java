package disco.ReGroup;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Regroup_reducer extends
		Reducer<IntWritable, Text, IntWritable, Text> {

	Text value = new Text();
	String job;

	int k, l;
	int num_machine;
	int[] value_change;

	int num, i;

	StringTokenizer st1, st2;

	MultipleOutputs<IntWritable, Text> mos;

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

		if (key.get() < 0) {
			if (job.equals("r"))
				value_change = new int[l];
			else
				value_change = new int[k];
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
			key.set(-(key.get() + 1));
			mos.write(key, value, "subMatrix");
			mos.write("assign", key, value,"subMatrix/subMatrix");
			
		} else {
			for (Text line : values) {
				mos.write("assign", key, line,"assign/assign");
			}
		}

	}

	private static String arrToString(int[] arr) {
		String ret = "";
		for (int d : arr) {
			ret += d + " ";
		}
		return ret;
	}

	@Override
	protected void cleanup(
			Reducer<IntWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		mos.close();
	}
}