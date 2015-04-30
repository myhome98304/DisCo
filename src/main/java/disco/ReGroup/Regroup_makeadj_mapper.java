package disco.ReGroup;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Regroup_makeadj_mapper extends
		Mapper<IntWritable, Text, IntWritable, Text> {
	Text ret = new Text();
	StringTokenizer st;
	int k, l;
	String job;
	int[] val;

	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		k = conf.getInt("k", 0);
		l = conf.getInt("l", 0);
		job = conf.get("job", "");
	}

	@Override
	protected void map(IntWritable key, Text value,
			Mapper<IntWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if (value.toString().startsWith("y")) {
			context.write(key, value);
		} else {

			st = new StringTokenizer(value.toString(), " ");
			ret.set(key.get() + " 1");
			if (job.equals("r"))
				val = new int[k];
			else
				val = new int[l];
			val[key.get()]++;
			ret.set(arrToString(val));
			while (st.hasMoreTokens()) {
				key.set(Integer.parseInt(st.nextToken()));

				context.write(key, ret);
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

}
