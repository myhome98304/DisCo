package old.disco.ReGroup;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Regroup_reducer extends
		Reducer<IntWritable, Text, IntWritable, Text> {

	IntWritable ret_key = new IntWritable();
	Text ret_value = new Text();
	String job;
	int k, l;
	int cluster;
	int num_machine;
	MultipleOutputs<IntWritable, Text> mos;

	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		job = conf.get("job", "");

		k = conf.getInt("k", 0);
		l = conf.getInt("l", 0);
		num_machine = conf.getInt("num_machine", 1);
		mos = new MultipleOutputs<IntWritable, Text>(context);
	}

	@Override
	protected void cleanup(
			Reducer<IntWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		mos.close();
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

		cluster = (key.get()-key.get()%num_machine)/num_machine;
		
		ret_value.set(cluster+"");
		
		StringTokenizer st1, st2;
		int num=0;
		while (lines.hasNext()) {
			int i = 0;
			st1 = new StringTokenizer(lines.next().toString(), "\t");
			num+= Integer.parseInt(st1.nextToken());
			
			st2 = new StringTokenizer(st1.nextToken(), " ");
			while (st2.hasMoreTokens())
				value_change[i++] += Long.parseLong(st2.nextToken());
			
			st2 = new StringTokenizer(st1.nextToken(), " ");
			while (st2.hasMoreTokens()){
				ret_key.set(Integer.parseInt(st2.nextToken()));
				mos.write("assign", ret_key,ret_value,"assign/assign" );
			}
		}

		/*
		 * Key : cluster number Value : nonzeros of each cluster corresponds to
		 * row cluster
		 */
		key.set(cluster);
		ret_value.set(num + "\t" + arrToString(value_change));
		mos.write("subM", key, ret_value,"subM/subM");

	}

	private static String arrToString(int[] arr) {
		String ret = "";
		for (int d : arr) {
			ret += d + " ";
		}
		return ret;
	}
}