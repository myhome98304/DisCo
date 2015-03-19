package disco.ReGroup;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class makeNew_split_mapper extends
		Mapper<LongWritable, Text, Text, Text> {
	String cluster;
	String index;
	StringTokenizer st1, st2;

	Text map_key = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		st1 = new StringTokenizer(value.toString(), "\t");
		index = st1.nextToken();
		cluster = st1.nextToken();
		try {
			int c = Integer.parseInt(cluster);
			map_key.set(index);
			value.set("x" + cluster);
			context.write(map_key, value);
		} catch (Exception e) {

			System.out.println(cluster+" "+value.toString());
			st2 = new StringTokenizer(st1.nextToken(), " ");
			while (st2.hasMoreTokens()) {
				map_key.set(st2.nextToken());
				value.set(cluster.substring(1));
				context.write(map_key, value);
			}
		}

	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
	}

}
