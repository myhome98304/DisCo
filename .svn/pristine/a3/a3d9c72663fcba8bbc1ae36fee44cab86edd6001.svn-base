package disco.ReGroup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Regroup_del_zero_mapper extends
		Mapper<IntWritable, Text, IntWritable, Text> {
	IntWritable map_key = new IntWritable();
	Text val = new Text();
	MultipleOutputs<IntWritable, Text> mos;

	boolean zero_cluster;
	Configuration conf;
	ArrayList<Integer> del_zero;
	String job;
	int index;
	int k, l;
	int i;
	int cluster;
	StringTokenizer st, st1;
	String[] split;
	String cand;
	String data;

	@Override
	protected void map(IntWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		st = new StringTokenizer(value.toString(), "\t");
		index = key.get();
		data = st.nextToken();
		map_key.set(index);
		if (data.equals(job)) {
			cluster = del_zero.get(Integer.parseInt(st.nextToken()));

			val.set(data + "\t" + cluster + "\t" + st.nextToken() + "\t"
					+ st.nextToken());
			
			mos.write(map_key, val, "assign1");

		} else {
			cluster = Integer.parseInt(st.nextToken());
			st1 = new StringTokenizer(st.nextToken(), " ");
			split = new String[job.equals("r") ? k : l];
			i = 0;
			while (st1.hasMoreTokens()) {

				cand = st1.nextToken();

				if (del_zero.get(i) > -1) {
					split[del_zero.get(i)] = cand;
				}
				i++;

			}

			val.set(data + "\t" + cluster + "\t" + arrToString(split) + "\t"
					+ st.nextToken());
			mos.write(map_key, val, "assign2");
		}

	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		conf = context.getConfiguration();
		mos = new MultipleOutputs<IntWritable, Text>(context);

		job = conf.get("job", "");
		k = conf.getInt("k", 0);
		l = conf.getInt("l", 0);
		del_zero = new ArrayList<>();
		i = 0;
		st = new StringTokenizer(conf.get("del_zero", ""), " ");

		while (st.hasMoreTokens())
			del_zero.add(Integer.parseInt(st.nextToken()));

	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		mos.close();
	}

	private static String arrToString(String[] arr) {
		String ret = "";
		for (String d : arr) {
			ret += d + " ";
		}
		return ret;
	}

}
