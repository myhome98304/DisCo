package disco.ReGroup;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Regroup_mapper extends
		Mapper<LongWritable, Text, IntWritable, Text> {
	int num_machine;

	IntWritable key = new IntWritable();
	Text value = new Text();
	String job;
	int[] row_permut;
	int[] col_permut;
	long[] rowSet;
	long[] colSet;
	int m, n;
	double[][] distribution;

	int k, l;

	@Override
	public void setup(Context context) {

		Configuration conf = context.getConfiguration();
		num_machine = conf.getInt("num_machine", 1);
		job = conf.get("job", "");
		String subMatrix_s = conf.get("subMatrix_String", "");

		k = conf.getInt("k", 0);
		m = conf.getInt("m", 0);
		n = conf.getInt("n", 0);
		l = conf.getInt("l", 0);
		rowSet = new long[k];
		colSet = new long[l];

		StringTokenizer st;

		int i;

		if (job.equals("c")) {
			row_permut = new int[m];
			i = 0;
			st = new StringTokenizer(conf.get("row_permut", ""), " ");
			while (st.hasMoreElements()) {
				row_permut[i++] = Integer.parseInt(st.nextToken());
			}
		} else {
			col_permut = new int[n];
			i = 0;
			st = new StringTokenizer(conf.get("col_permut", ""), " ");
			while (st.hasMoreElements()) {
				col_permut[i++] = Integer.parseInt(st.nextToken());
			}
		}

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
	public void map(LongWritable arg0, Text line, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String job = this.job;
		StringTokenizer st = new StringTokenizer(line.toString(), " \t");

		double curValue = 0;
		double temp = Double.MAX_VALUE;

		if (job.equals("r")) {

			int r = 0;

			int row = Integer.parseInt(st.nextToken());
			int[] row_splice = new int[l];

			/* Parse line to adjacency list */
			while (st.hasMoreTokens()) {
				try {
					row_splice[col_permut[Integer.parseInt(st.nextToken())]]++;
				} catch (Exception e) {
					System.out.println("Out of Range");
				}

			}
			
			/*
			 * Set line to each row cluster and calculate cost Select cluster
			 * that minimizes cost
			 */
			double cur;
			for (int i = 0; i < k; i++) {
				cur = 0;
				for (int j = 0; j < l; j++) {
					curValue = distribution[i][j];
					cur +=codeCost(row_splice[j],colSet[j],distribution[i][j]);
					
				}
				if (temp > cur) {
					r = i;
					temp = cur;
				}
			}

			/*
			 * Key : cluster number Value : row number + spliced adjacency list
			 */
			key.set(r);
			value.set(1 + "\t" + arrToString(row_splice) + "\t" + row);

			context.write(key, value);
		}

		else {

			int c = 0;

			int col = Integer.parseInt(st.nextToken());

			int[] col_splice = new int[k];

			/* Parse line to adjacency list */
			while (st.hasMoreTokens()) {
				try {
					col_splice[row_permut[Integer.parseInt(st.nextToken())]]++;
				} catch (Exception e) {
					System.out.println("Out of Range");
				}

			}
			

			/*
			 * Set line to each column cluster and calculate cost Select cluster
			 * that minimizes cost
			 */
			double cur;
			for (int i = 0; i < l; i++) {
				cur = 0;
				for (int j = 0; j < k; j++) {

					curValue = distribution[j][i];
					cur += codeCost(col_splice[j], rowSet[j],
							distribution[j][i]);
				}
				if (temp > cur) {
					c = i;
					temp = cur;
				}
			}

			/*
			 * Key : cluster number Value : row number + spliced adjacency list
			 */
			key.set(c);
			value.set(1 + "\t" +  arrToString(col_splice) + "\t" +col);

			context.write(key, value);

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