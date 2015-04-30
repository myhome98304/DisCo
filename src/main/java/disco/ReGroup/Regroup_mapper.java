package disco.ReGroup;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Regroup_mapper extends Mapper<IntWritable, Text, IntWritable, Text> {
	int num_machine;

	IntWritable key = new IntWritable();
	Text value = new Text();
	String job;

	long[] rowSet;
	long[] colSet;
	boolean redo = false;
	double[][] distribution;

	int k, l;
	int cluster, index;
	int i;
	int[] splice;

	StringTokenizer st1, st2;
	String data;
	double curValue;
	double cur, temp;
	String adj;
	MultipleOutputs<IntWritable, Text> mos;

	@Override
	protected void cleanup(
			Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		mos.close();
	}

	
	@Override
	public void setup(Context context) {

		Configuration conf = context.getConfiguration();
		mos = new MultipleOutputs<IntWritable, Text>(context);

		
		num_machine = conf.getInt("num_machine", 1);
		job = conf.get("job", "");
		String subMatrix_s = conf.get("subMatrix_String", "");

		k = conf.getInt("k", 0);
		l = conf.getInt("l", 0);
		rowSet = new long[k];
		colSet = new long[l];
		StringTokenizer st;
		redo = conf.getBoolean("redo", false);
		int i;

		i = 0;
		st = new StringTokenizer(conf.get("rowSet", ""), "[,] ");
		while (st.hasMoreTokens()) {
			rowSet[i++] = Long.parseLong(st.nextToken());
		}

		i = 0;
		st = new StringTokenizer(conf.get("colSet", ""), "[,] ");
		while (st.hasMoreTokens()) {
			colSet[i++] = Long.parseLong(st.nextToken());
		}

		i = 0;
		String temp;

		st = new StringTokenizer(subMatrix_s, "{}\t ");

		distribution = new double[k][l];

		long size;
		long value;
		if (!redo)
			while (st.hasMoreElements()) {
				temp = st.nextToken();

				String[] cand = temp.split(",");
				for (int j = 0; j < l; j++) {
					size = rowSet[i] * colSet[j];
					value = Long.parseLong(cand[j]);
					distribution[i][j] = size == 0 || value == 0 ? 0
							: (double) value / size;
				}
				i++;
			}
	}

	@Override
	public void map(IntWritable arg0, Text line, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
	

		st1 = new StringTokenizer(line.toString(), "\t");
		index = arg0.get();

		data = st1.nextToken();

		cluster = Integer.parseInt(st1.nextToken());

		if (data.equals(job)) {
			curValue = 0;
			temp = Double.MAX_VALUE;

			if (job.equals("r"))
				splice = new int[l];
			else
				splice = new int[k];

			st2 = new StringTokenizer(st1.nextToken(), " ");

			i = 0;
			while (st2.hasMoreTokens()) {
				splice[i++] = Integer.parseInt(st2.nextToken());
			}

			/*
			 * Set line to each row cluster and calculate cost Select cluster
			 * that minimizes cost
			 */
			if (!redo) {
				if (job.equals("r")) {
					for (int i = 0; i < k; i++) {
						cur = 0;
						for (int j = 0; j < l; j++) {
							curValue = distribution[i][j];
							cur += codeCost(splice[j], colSet[j],
									distribution[i][j]);

						}
						if (temp > cur) {
							cluster = i;
							temp = cur;
						}
					}
				} else {

					for (int i = 0; i < l; i++) {
						cur = 0;
						for (int j = 0; j < k; j++) {

							curValue = distribution[j][i];
							cur += codeCost(splice[j], rowSet[j],
									distribution[j][i]);
						}
						if (temp > cur) {
							cluster = i;
							temp = cur;
						}
					}
				}

			}
			/*
			 * Key : cluster number Value : row number + spliced adjacency list
			 */

			adj = st1.nextToken();
			

			key.set(+ (cluster * num_machine + (int) (num_machine * Math
							.random())));
			value.set(1 + "\t" + arrToString(splice));
			mos.write("subM", key, value, "subM/subM");

			key.set(index);
			value.set(job + "\t" + cluster + "\t" + arrToString(splice) + "\t"
					+ adj);
			mos.write("assign1", key, value, "assign/assign1");

			key.set(cluster);
			value.set(adj);
			mos.write("calc",key,value,"calc/calc");

		}

		else {
			st1.nextToken();

			key.set(index);
			value.set("y" + cluster + "\t" + st1.nextToken());
			mos.write("calc", key, value, "calc/calc");
		}
	}

	private static double codeCost(long nonzero, long size, double density) {
		return (nonzero == 0 ? 0 : nonzero * Math.log(1 / density))
				+ ((size - nonzero == 0) ? 0 : (size - nonzero)
						* Math.log(1 / (1 - density)));
	}

	private static String arrToString(int[] arr) {
		String ret = "";
		for (int d : arr) {
			ret += d + " ";
		}
		return ret;
	}

}