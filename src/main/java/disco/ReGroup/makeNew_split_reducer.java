package disco.ReGroup;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class makeNew_split_reducer extends Reducer<Text, Text, Text, Text> {
	Configuration conf;
	String job;
	int[] split;
	int i, k, l;
	Text value = new Text();
	String cluster;
	StringTokenizer st;

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		if (job.equals("r")) {
			split = new int[k];
		} else
			split = new int[l];

		for (Text line : values) {
			if (line.toString().startsWith("x"))
				cluster = line.toString();

			else {
				i = 0;
				st = new StringTokenizer(line.toString()," ");
				while (st.hasMoreTokens()) {
					split[i++] += Integer.parseInt(st.nextToken());
				}
			}
		}
		value.set(cluster.substring(1) + "\t" + arrToString(split));
		context.write(key, value);
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		conf = context.getConfiguration();
		job = conf.get("job", "");
		k = conf.getInt("k", 0);
		l = conf.getInt("l", 0);

	}

	private static String arrToString(int[] arr) {
		String ret = "";
		for (int d : arr) {
			ret += d + " ";
		}
		return ret;
	}
}
