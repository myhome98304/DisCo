package disco.IncCluster;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class IncCluster_mapper extends
		Mapper<IntWritable, Text, IntWritable, Text> {
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
	int num_machine;
	int i;
	MultipleOutputs<IntWritable, Text> mos;

	@Override
	protected void cleanup(
			Mapper<IntWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		mos.close();
	}

	@Override
	public void setup(Context context) {

		Configuration conf = context.getConfiguration();
		mos = new MultipleOutputs<IntWritable, Text>(context);

		job = conf.get("job", "");

		String subMatrix_s = conf.get("subMatrix_String", "");

		max_Shannon = conf.getInt("max_Shannon", 0);
		num_machine = conf.getInt("num_machine", 1);
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
	public void map(IntWritable arg0, Text line, Context context)
			throws IOException, InterruptedException {
		st1 = new StringTokenizer(line.toString(), "\t");
		index = arg0.get();
		st1.nextToken();
		cluster = Integer.parseInt(st1.nextToken());

		key.set(index);
		subM = st1.nextToken();

		if (cluster != max_Shannon) {
			value.set(job + "\t" + cluster + "\t" + subM + "\t"
					+ st1.nextToken());
			mos.write("assign", key, value, "assign/assign");
			return;
		}

		i = 0;

		st2 = new StringTokenizer(subM, " ");

		if (job.equals("r")) {
			/* If given row is not in the target cluster, return */

			subM_change = new long[l];
			numof_maxShannon = rowSet[max_Shannon];

			while (st2.hasMoreTokens())
				subM_change[i++] = Long.parseLong(st2.nextToken());

			partSum_aft = 0;

			/* Calculate Cost of cluster after deleting given row */
			for (int c = 0; c < l; c++)
				partSum_aft += codeCost(numof_maxShannon - 1, colSet[c],
						subMatrix[max_Shannon][c] - subM_change[c]);

			partSum_aft /= numof_maxShannon - 1;

			if (partSum_aft < partSum_bef) {
				value.set(job + "\t" + k + "\t" + subM + "\t" + st1.nextToken());
				mos.write("assign", key, value, "assign/assign");
				key.set((-1 - (int) (num_machine * Math.random())));
				value.set(1 + "\t" + subM);
				mos.write("calc", key, value, "calc/calc");
			}

			else {
				value.set(job + "\t" + cluster + "\t" + subM + "\t"
						+ st1.nextToken());
				mos.write("assign", key, value, "assign/assign");
			}

		} else {
			/* If given column is not in the target cluster, return */

			subM_change = new long[k];

			numof_maxShannon = colSet[max_Shannon];

			while (st2.hasMoreTokens())
				subM_change[i++] = Long.parseLong(st2.nextToken());

			partSum_aft = 0;

			for (int r = 0; r < k; r++)
				partSum_aft += codeCost(numof_maxShannon - 1, rowSet[r],
						subMatrix[r][max_Shannon] - subM_change[r]);

			partSum_aft /= numof_maxShannon - 1;

			if (partSum_aft < partSum_bef) {
				value.set(job + "\t" + l + "\t" + subM + "\t" + st1.nextToken());
				mos.write("assign", key, value, "assign/assign");
				key.set((-1 - (int) (num_machine * Math.random())));
				value.set(1 + "\t" + subM);
				mos.write("calc", key, value, "calc/calc");
			}

			else {
				value.set(job + "\t" + cluster + "\t" + subM + "\t"
						+ st1.nextToken());
				mos.write("assign", key, value, "assign/assign");
			}
		}

		/*
		 * If Delete a line decreases the cost, report to reducer Key : trash
		 * value 1 Value : row or column number + splitted adjacency list
		 */

	}

	private static double codeCost(long m, long n, double total_weight) {
		// if (m == 0 || n == 0 || total_weight == 0 || total_weight == m * n)
		// return 0;
		double prob = (total_weight) / (m * n);

		return (m * n)
				* ((prob == 0 ? 0 : prob * Math.log(1 / prob)) + (1 - prob == 0 ? 0
						: (1 - prob) * Math.log(1 / (1 - prob))));
	}

}
