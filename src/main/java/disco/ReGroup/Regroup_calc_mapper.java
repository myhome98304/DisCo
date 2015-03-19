package disco.ReGroup;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Regroup_calc_mapper extends Mapper<LongWritable, Text, Text, Text> {
	Text map_key = new Text();
	Text value = new Text();

	boolean zero_cluster;
	Configuration conf;
	int[] del_zero;
	String job;
	int k, l;
	int i;
	int cluster;
	StringTokenizer st;
	String[] split;

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		split = value.toString().split("\t");
		if (split.length == 2) {
			map_key.set(split[0]);
			value.set(split[1]);
			context.write(map_key, value);
		} else {
			map_key.set(split[0]);
			if (zero_cluster) {
				cluster = del_zero[Integer.parseInt(split[1])];
				value.set("x"+cluster);
				context.write(map_key, value);

				map_key.set("x" + split[0]);
				value.set(cluster + "\t" + split[2]);
				context.write(map_key, value);
			} else {
				value.set("x"+split[1]);
				context.write(map_key, value);
			}

		}

	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		conf = context.getConfiguration();
		zero_cluster = conf.getBoolean("zero_cluster", false);

		if (zero_cluster) {
			job = conf.get("job", "");

			if (job.equals("r")) {
				k = conf.getInt("k", 0);
				del_zero = new int[k];
			} else {
				l = conf.getInt("l", 0);
				del_zero = new int[l];
			}

			i = 0;
			st = new StringTokenizer(conf.get("del_zero", ""), " ");

			while (st.hasMoreTokens())
				del_zero[i++] = Integer.parseInt(st.nextToken());
		}
	}

}
