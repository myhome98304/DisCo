package disco.ReGroup;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Regroup_calc_reducer extends Reducer<Text, Text, Text, Text> {
	MultipleOutputs<Text, Text> mos;
	boolean zero_cluster;
	Configuration conf;
	String cluster;
	String ret;
	Text value = new Text();

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		mos.close();
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if (zero_cluster) {
			if (key.toString().startsWith("x")) {
				key.set(key.toString().split("x")[0]);
				for (Text line : values) {
					mos.write("nonzero", key, line, "nonzero/nonzero");
				}
			}
		}

		ret = "";
		for (Text line : values) {
			if (line.toString().startsWith("x")) {
				cluster = line.toString();
			} else {
				ret += line.toString();
			}
		}
		
		if(key.toString().isEmpty())
			return;

		value.set(cluster + "\t" + ret);
		mos.write("temp", key, value, "temp/temp");

	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		mos = new MultipleOutputs<Text, Text>(context);
		conf = context.getConfiguration();
		zero_cluster = conf.getBoolean("zero_cluster", false);
	}

}
