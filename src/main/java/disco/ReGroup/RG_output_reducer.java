package disco.ReGroup;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class RG_output_reducer extends
		Reducer<IntWritable, Text, IntWritable, Text> {

	private MultipleOutputs<IntWritable, Text> mos;
	int k, l;
	long[] Set;
	IntWritable k1 = new IntWritable();
	StringTokenizer st;
	int number;
	String cur_job;
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		mos.close();
	}

	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		if (key.get() > -1) {
			int index;
			number = 0;
			Set = new long[cur_job.equals("r") ? l : k];
			

			for (Text line : values) {
				st = new StringTokenizer(line.toString(), "\t ");
				number += Integer.parseInt(st.nextToken());
				index = 0;

				while (st.hasMoreTokens())
					Set[index++] += Long.parseLong(st.nextToken());

			}
			mos.write(key, new Text(number + "\t" + arrToString(Set)),
					"subMatrix");
		}

		else {

			mos.write(new IntWritable(-(key.get() + 1)), values.iterator()
					.next(), "assign");
		}

	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = context.getConfiguration();
		mos = new MultipleOutputs<IntWritable, Text>(context);
		k = conf.getInt("k", 0);
		l = conf.getInt("l", 0);
		cur_job=conf.get("job","");
	}

	private static String arrToString(long[] arr) {
		String ret = "";
		for (long d : arr) {
			ret += d + " ";
		}
		return ret;
	}
}