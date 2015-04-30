package old.disco.ReGroup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.LineReader;

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

	Configuration conf;
	String outputPath;
	Text read = new Text();
	int k, l;

	@Override
	public void setup(Context context) throws IOException {

		conf = context.getConfiguration();
		outputPath = conf.get("outputPath","");

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
			for(LineReader lr : readLine(new Path(outputPath+"/preProcess/r/assign"))){
				while(lr.readLine(read)>0){
					
					st = new StringTokenizer(read.toString(), "\t");
					row_permut[Integer.parseInt(st.nextToken())] = Integer.parseInt(st.nextToken());
				}
			}
		} else {
			col_permut = new int[n];
			for(LineReader lr : readLine(new Path(outputPath+"/preProcess/c/assign"))){
				while(lr.readLine(read)>0){
					st = new StringTokenizer(read.toString(), "\t");
					col_permut[Integer.parseInt(st.nextToken())] = Integer.parseInt(st.nextToken());
				}
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
			while (st.hasMoreElements())
				row_splice[col_permut[Integer.parseInt(st.nextToken())]]++;

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
			while (st.hasMoreElements())
				col_splice[row_permut[Integer.parseInt(st.nextToken())]]++;

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
			key.set(c*num_machine+(int)(Math.random()*num_machine));
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
	public  ArrayList<LineReader> readLine(Path location)
			throws IOException {
		FileSystem fileSystem = FileSystem.get(location.toUri(), conf);
		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		FileStatus[] items = fileSystem.listStatus(location);
		ArrayList<LineReader> readLines = new ArrayList<>();
		for (FileStatus item : items) {

			// ignoring files like _SUCCESS
			if (item.getPath().getName().startsWith("_")) {
				continue;
			} else {
				readLines.add(new LineReader(fileSystem.open(item.getPath())));
			}
		}
		return readLines;
	}

}