package disco.IncDimension;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IncDimension_combiner extends
		Reducer<IntWritable, Text, IntWritable, Text> {
	String job;
	int k, l;
	int i;
	Text val = new Text();
	Text assign = new Text();
	long[] subM_change;
	StringTokenizer st1, st2;
	String ret;
	int num;
	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		job = conf.get("job", "");

		k = conf.getInt("k", 0);
		l = conf.getInt("l", 0);
	}

	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		if (key.get() < 0) {
			if (job.equals("r")) {
				subM_change = new long[l];
			} else {
				subM_change = new long[k];
			}

			num=0;
			for (Text line : values) {
				
				st1 = new StringTokenizer(line.toString(), "\t");
				num+= Integer.parseInt(st1.nextToken());
				i = 0;
				st2 = new StringTokenizer(st1.nextToken(), " ");
				
				while (st2.hasMoreTokens())
					subM_change[i++] += Long.parseLong(st2.nextToken());

			}

			val.set(num+"\t"+arrToString(subM_change));
			context.write(key, val);
		}

		/*
		 * Report (row or column numbers to split) + (values of decreased
		 * nozeros in each cluster after split)
		 */
		else{
			for (Text line : values) {
				context.write(key,line);
			}	
		}
		

	}

	private static String arrToString(long[] arr) {
		String ret = "";
		for (long d : arr) {
			ret += d + " ";
		}
		return ret;
	}

}
