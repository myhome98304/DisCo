package disco.IncDimension;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IncDimension_random_mapper extends
		Mapper<LongWritable, Text, IntWritable, Text> {
	IntWritable key = new IntWritable();
	Text value = new Text();
	String job;

	int k, l;

	long[] rowSet;
	long[] colSet;

	long[][] subMatrix;
	long[] subM_change;
	String subM;

	int index;
	int cluster;

	double partSum_aft;
	double partSum_bef = 0;

	long numof_maxShannon;
	int max_Shannon;

	StringTokenizer st1;
	StringTokenizer st2;

	int i;

	@Override
	public void setup(Context context) {

		Configuration conf = context.getConfiguration();

		job = conf.get("job", "");

		String subMatrix_s = conf.get("subMatrix_String", "");

		max_Shannon = conf.getInt("max_Shannon", 0);

		k = conf.getInt("k", 0);
		l = conf.getInt("l", 0);

		rowSet = new long[k];
		colSet = new long[l];

		partSum_bef = Double.parseDouble(conf.get("partSum_bef", ""));

		StringTokenizer st;

		i = 0;
		st = new StringTokenizer(conf.get("rowSet", ""), "[,] ");
		while (st.hasMoreTokens())
			rowSet[i++] = Long.parseLong(st.nextToken());

		i = 0;
		st = new StringTokenizer(conf.get("colSet", ""), "[,] ");
		while (st.hasMoreTokens())
			colSet[i++] = Long.parseLong(st.nextToken());

		st = new StringTokenizer(subMatrix_s, "{}\t ");

		i = 0;
		String temp;

		subMatrix = new long[k][l];

		while (st.hasMoreElements()) {
			temp = st.nextToken();
			String[] cand = temp.split(",");
			for (int j = 0; j < l; j++) {
				subMatrix[i][j] = (int) Float.parseFloat(cand[j]);
			}
			i++;
		}
	}

	@Override
	public void map(LongWritable arg0, Text line, Context context)
			throws IOException, InterruptedException {

		st1 = new StringTokenizer(line.toString(), "\t ");

		index = Integer.parseInt(st1.nextToken());
		cluster = Integer.parseInt(st1.nextToken());

		key.set(index);
		subM = st1.nextToken();

		if (cluster != max_Shannon) {
			value.set(cluster + "\t" + subM);
			context.write(key, value);
			return;
		}

		i = 0;

		st2 = new StringTokenizer(subM, " ");

		if (job.equals("r"))
			subM_change = new long[l];
		else
			subM_change = new long[k];

		while (st2.hasMoreTokens())
			subM_change[i++] = Long.parseLong(st2.nextToken());

		/*
		 * If Delete a line decreases the cost, report to reducer Key : trash
		 * value 1 Value : row or column number + splitted adjacency list
		 */
		if (Math.random() < 0.5) {
			value.set(k + "\t" + subM);
			context.write(key, value);
			key.set(-1);
			value.set(1 + "\t" + subM);
			context.write(key, value);
		}

		else {
			value.set(cluster + "\t" + subM);
			context.write(key, value);
		}

	}

}
