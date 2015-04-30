package old.disco.ReGroup;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Regroup_combiner extends
		Reducer<IntWritable, Text, IntWritable, Text> {

	IntWritable key = new IntWritable();
	String job;
	int k, l;
	int num_machine;
	int num;
	StringTokenizer st1, st2;
	int[] value_change;
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

		
		String ret = "";
		Iterator<Text> lines = values.iterator();
		
		if (job.equals("r"))
			value_change = new int[l];
		else
			value_change = new int[k];

		
		num=0;
		for(Text line : values){
			int i = 0;
			st1 = new StringTokenizer(line.toString(), "\t");
			num+= Integer.parseInt(st1.nextToken());
			st2 = new StringTokenizer(st1.nextToken(), " ");
			while (st2.hasMoreTokens())
				value_change[i++] += Long.parseLong(st2.nextToken());
			ret += st1.nextToken()+" ";
		}

		/*
		 * Key : cluster number Value : nonzeros of each cluster corresponds to
		 * row cluster
		 */
		
		context.write(key, new Text(num + "\t" + arrToString(value_change) + "\t"
				+ ret));

	}

	private static String arrToString(int[] arr) {
		String ret = "";
		for (int d : arr) {
			ret += d + " ";
		}
		return ret;
	}
}