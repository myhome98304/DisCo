package disco.ReGroup;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class makeNew_split_comibner extends Reducer<Text, Text, Text, Text> {
	Configuration conf;
	int k, l;
	int[] split;
	String job;
	String[] calc;
	Text value = new Text();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		if (job.equals("r")) {
			split = new int[k];
		}
		else
			split = new int[l];
		
		for (Text line : values) {

			if (line.toString().startsWith("x")) {
				context.write(key, line);
			} else {
				split[Integer.parseInt(line.toString())]++;
			}
		}
		value.set(arrToString(split));
		context.write(key, value);

	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		conf = context.getConfiguration();
		job = conf.get("job", "");
		
		if (job.equals("r"))
			k = conf.getInt("k", 0);
		else
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
