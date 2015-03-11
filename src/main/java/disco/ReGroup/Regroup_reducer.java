package disco.ReGroup;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Regroup_reducer extends
		Reducer<IntWritable, Text, IntWritable, Text> {

	IntWritable key = new IntWritable();
	String job;
	int k, l;
	int num_machine;

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

		int[] value_change;
		String ret = "";
		Iterator<Text> lines = values.iterator();
		
		if (job.equals("r"))
			value_change = new int[l];
		else
			value_change = new int[k];

		StringTokenizer st1, st2;

		while (lines.hasNext()) {
			int i = 0;
			st1 = new StringTokenizer(lines.next().toString(), "\t");
			ret += st1.nextToken()+" ";
			st2 = new StringTokenizer(st1.nextToken(), " ");
			while (st2.hasMoreTokens())
				value_change[i++] += Long.parseLong(st2.nextToken());
		}

		/*
		 * Key : cluster number Value : nonzeros of each cluster corresponds to
		 * row cluster
		 */
		
		context.write(key, new Text("\t" + ret + "\t"
				+ arrToString(value_change)));

	}

	private static String arrToString(int[] arr) {
		String ret = "";
		for (int d : arr) {
			ret += d + " ";
		}
		return ret;
	}
}