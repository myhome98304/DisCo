package disco.IncCluster;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IncCluster_random_mapper extends
		Mapper<LongWritable, Text, IntWritable, Text> {
	IntWritable key = new IntWritable();
	Text value = new Text();
	String job;

	String subM;

	int index;
	int cluster;

	int max_Shannon;

	StringTokenizer st1;
	int num_machine;
	int i;
	int k, l;

	@Override
	public void setup(Context context) {

		Configuration conf = context.getConfiguration();

		job = conf.get("job", "");

		max_Shannon = conf.getInt("max_Shannon", 0);
		num_machine = conf.getInt("num_machine", 1);
		k = conf.getInt("k", k);
		l = conf.getInt("l", l);
	}

	@Override
	public void map(LongWritable arg0, Text line, Context context)
			throws IOException, InterruptedException {

		st1 = new StringTokenizer(line.toString(), "\t");
		index = Integer.parseInt(st1.nextToken());
		st1.nextToken();
		cluster = Integer.parseInt(st1.nextToken());

		key.set(index);
		subM = st1.nextToken();
		
		
		if (cluster != max_Shannon) {
			value.set(job + "\t" + cluster + "\t" + subM + "\t"
					+ st1.nextToken());
			context.write(key, value);
			return;
		}

		i = 0;

		if (job.equals("r")) {

			if (Math.random() < 0.5) {
				value.set(job + "\t" + k + "\t" + subM + "\t" + st1.nextToken());
				context.write(key, value);
				key.set((-1 - (int) (num_machine * Math.random())));
				value.set(1 + "\t" + subM);
				context.write(key, value);
			}

			else {
				value.set(job + "\t" + cluster + "\t" + subM + "\t"
						+ st1.nextToken());
				context.write(key, value);
			}

		} else {
			/* If given column is not in the target cluster, return */

			if (Math.random() < 0.5) {
				value.set(job + "\t" + l + "\t" + subM + "\t" + st1.nextToken());
				context.write(key, value);
				key.set((-1 - (int) (num_machine * Math.random())));
				value.set(1 + "\t" + subM);
				context.write(key, value);
			}

			else {
				value.set(job + "\t" + cluster + "\t" + subM + "\t"
						+ st1.nextToken());
				context.write(key, value);
			}
		}

		/*
		 * If Delete a line decreases the cost, report to reducer Key : trash
		 * value 1 Value : row or column number + splitted adjacency list
		 */

	}

}
