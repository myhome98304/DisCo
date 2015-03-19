package disco;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.LineReader;

import disco.IncDimension.IncDiemnsion_mapper;
import disco.IncDimension.IncDimension_random_mapper;
import disco.IncDimension.IncDimension_reducer;
import disco.IncDimension.naive_partitioner_inc;
import disco.ReGroup.Regroup_calc_mapper;
import disco.ReGroup.Regroup_calc_reducer;
import disco.ReGroup.Regroup_combiner;
import disco.ReGroup.Regroup_mapper;
import disco.ReGroup.Regroup_reducer;
import disco.ReGroup.makeNew_split_comibner;
import disco.ReGroup.makeNew_split_mapper;
import disco.ReGroup.makeNew_split_reducer;
import disco.ReGroup.naive_partitioner_reg;
import disco.preProcess.Preprocess_c_mapper;
import disco.preProcess.Preprocess_combiner;
import disco.preProcess.Preprocess_r_mapper;
import disco.preProcess.Preprocess_reducer;
import disco.preProcess.Preprocess_sum_mapper;
import disco.preProcess.Preprocess_sum_reducer;
import disco.preProcess.naive_partitioner;

public class CA {

	static String inputFile;
	static String outputPath;
	static double Cost = 0;
	static Configuration conf;
	static int num_machine = 1;

	static ArrayList<Long> rowSet;
	static ArrayList<Long> colSet;
	static double[][] codeMatrix;
	static long[][] subMatrix;
	static int k;
	static int l;

	static int m;
	static int n;

	static double row_size;
	static double col_size;

	static boolean data;
	static boolean mode;
	static boolean setSize;
	static boolean random = false;
	static boolean zero_cluster;

	static FileSystem fs;
	static PrintWriter fw;
	static ArrayList<Double> cost_show = new ArrayList<>();

	static Path row_nonzero;
	static Path col_nonzero;
	static Path row_adj;
	static Path col_adj;
	static Path r_inc;
	static Path c_inc;
	static Path r_reg;
	static Path c_reg;

	/**
	 * make adj_list, calculate total Weight
	 * 
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void makeAdjList() throws IOException,
			ClassNotFoundException, InterruptedException {

		/* Make Adjacency List from (src, dst) type data set */
		conf.setInt("num_machine", num_machine);

		Job job = new Job(conf);

		Path output = new Path(outputPath + "/preProcess/r");
		job.setJarByClass(CA.class);

		job.setJobName("preprocessing row data");

		FileInputFormat.addInputPath(job, new Path(inputFile));
		FileOutputFormat.setOutputPath(job, output);

		MultipleOutputs.addNamedOutput(job, "adj", TextOutputFormat.class,
				IntWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "nonzero", TextOutputFormat.class,
				IntWritable.class, Text.class);

		job.setMapperClass(Preprocess_r_mapper.class);
		job.setCombinerClass(Preprocess_combiner.class);
		job.setReducerClass(Preprocess_reducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(num_machine);
		job.waitForCompletion(true);

		job = new Job(conf);

		output = new Path(outputPath + "/preProcess/c");

		job.setJarByClass(CA.class);

		job.setJobName("preprocessing column data");

		FileInputFormat.addInputPath(job, new Path(outputPath
				+ "/preProcess/r/adj"));
		FileOutputFormat.setOutputPath(job, output);

		MultipleOutputs.addNamedOutput(job, "adj", TextOutputFormat.class,
				IntWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "nonzero", TextOutputFormat.class,
				IntWritable.class, Text.class);

		job.setMapperClass(Preprocess_c_mapper.class);
		job.setCombinerClass(Preprocess_combiner.class);
		job.setReducerClass(Preprocess_reducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(num_machine);
		job.waitForCompletion(true);

		/* Calculate Total Nonzeros */
		job = new Job(conf);

		output = new Path(outputPath + "/preProcess/sum");

		job.setJarByClass(CA.class);

		job.setJobName("preprocessing row data");

		FileInputFormat.addInputPath(job, new Path(outputPath
				+ "/preProcess/r/nonzero"));
		FileOutputFormat.setOutputPath(job, output);

		job.setMapperClass(Preprocess_sum_mapper.class);
		job.setCombinerClass(Preprocess_sum_reducer.class);
		job.setReducerClass(Preprocess_sum_reducer.class);
		job.setPartitionerClass(naive_partitioner.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(LongWritable.class);

		job.setNumReduceTasks(num_machine);
		job.waitForCompletion(true);

	}

	/**
	 * Increase the current cluster's dimension
	 * 
	 * @param cur_job
	 *            : cur_job:"r" or "c", specify row or column job
	 * @return true: dimension increased, false: dimension not increased
	 * @throws Exception
	 */
	public static boolean inc_Dimension(String cur_job) throws Exception {
		System.out.println("Increase Dimension starts with " + cur_job);
		Path cur_job_output;
		double partSum_bef = 0;

		FileSystem fs = FileSystem.get(conf);

		int i, j;
		double temp;
		double cur_cost;
		int max_Shannon;
		long num_candidates = 0;

		/*
		 * calculate which permutation set has maximum code cost per row or
		 * column
		 */
		if (cur_job.equals("r")) {

			fs.delete(r_inc, true);
			cur_job_output = r_inc;

			max_Shannon = 0;
			temp = 0;

			for (i = 0; i < k; i++) {

				cur_cost = 0;

				if (rowSet.get(i) != 0) {
					for (j = 0; j < l; j++) {
						cur_cost += codeMatrix[i][j];
					}

					cur_cost /= rowSet.get(i);
				}

				if (cur_cost > temp) {
					temp = cur_cost;
					max_Shannon = i;
					num_candidates = rowSet.get(i);
				}
			}

			partSum_bef = temp;

		} else {

			fs.delete(c_inc, true);
			cur_job_output = c_inc;

			max_Shannon = 0;
			temp = 0;

			for (i = 0; i < l; i++) {

				cur_cost = 0;

				if (colSet.get(i) != 0) {
					for (j = 0; j < k; j++) {
						cur_cost += codeMatrix[j][i];
					}

					cur_cost /= colSet.get(i);
				}

				if (cur_cost > temp) {
					temp = cur_cost;
					max_Shannon = i;
					num_candidates = colSet.get(i);
				}
			}

			partSum_bef = temp;
		}

		System.out.println("Sending Parameter");

		/* Start MapReduce */
		conf.setInt("l", l);

		conf.setInt("k", k);

		conf.set("partSum_bef", Double.toString(partSum_bef));

		conf.setInt("max_Shannon", max_Shannon);

		conf.set("subMatrix_String", matrixToString(subMatrix));

		conf.set("rowSet", rowSet.toString());

		conf.set("colSet", colSet.toString());

		conf.set("job", cur_job);

		System.out.println("Parameter sent");

		Job job = new Job(conf);
		job.setJarByClass(CA.class);

		job.setJobName("incDimension" + " " + cur_job);

		FileInputFormat.addInputPath(job, new Path(outputPath + "/preProcess/"
				+ cur_job + "/nonzero"));
		FileOutputFormat.setOutputPath(job, cur_job_output);

		/*
		 * If Matrix is homogeneous or IncDimension algorithms just sends all
		 * nodes to not move or move all, Use random split
		 */
		if (!random)
			job.setMapperClass(IncDiemnsion_mapper.class);
		else
			job.setMapperClass(IncDimension_random_mapper.class);

		job.setReducerClass(IncDimension_reducer.class);
		job.setPartitionerClass(naive_partitioner_inc.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		MultipleOutputs.addNamedOutput(job, "subMatrix",
				TextOutputFormat.class, IntWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "assign", TextOutputFormat.class,
				IntWritable.class, Text.class);

		job.setNumReduceTasks(num_machine);

		System.out.println("Start MapReduce");
		job.waitForCompletion(true);

		long[] subM_change;

		if (cur_job.equals("r"))
			subM_change = new long[l];
		else
			subM_change = new long[k];

		StringTokenizer st1, st2;

		int number = 0;
		ArrayList<LineReader> outs;

		try {
			/*
			 * SubMatrix-r-000xx files contains number of lines and Nonzeros to
			 * be removed
			 */
			outs = readLine(cur_job_output, "s");
			Text read = new Text();
			for (LineReader lr : outs) {

				while (lr.readLine(read) > 0) {

					if (read.getLength() == 0)
						continue;

					st1 = new StringTokenizer(read.toString(), "\t");
					st1.nextToken();

					number += Integer.parseInt(st1.nextToken());

					i = 0;
					st2 = new StringTokenizer(st1.nextToken(), " ");
					while (st2.hasMoreTokens())
						subM_change[i++] += Long.parseLong(st2.nextToken());

					read.clear();

				}
			}

		} catch (Exception e) {
			System.out.println("incdimesion");
		}

		/* If the algorithm removes all lines, return false */
		if (number == num_candidates)
			return false;

		long[][] subMatrix_temp;
		double[][] codeMatrix_temp;

		double Cost_temp = 0;
		double new_cost;

		if (cur_job.equals("r")) {

			/*
			 * If user set the minimum size of cluster, check whether the size
			 * fits
			 */
			if (number < m * row_size && setSize)
				return false;

			k++;

			subMatrix_temp = new long[k][l];
			codeMatrix_temp = new double[k][l];
			ArrayList<Long> Set_temp = new ArrayList<>();

			/* Update the subMatrix, codeMatrix */
			for (i = 0; i < k; i++) {
				if (i != k - 1 && i != max_Shannon)
					Set_temp.add(rowSet.get(i));
				else if (i == max_Shannon)
					Set_temp.add(rowSet.get(max_Shannon) - number);
				else
					Set_temp.add((long) number);

				if (i != max_Shannon && i != k - 1) {
					subMatrix_temp[i] = subMatrix[i];
					for (j = 0; j < l; j++) {
						codeMatrix_temp[i][j] = codeMatrix[i][j];
						Cost_temp += codeMatrix_temp[i][j];
					}
				}

				else if (i == max_Shannon) {
					for (j = 0; j < l; j++) {
						subMatrix_temp[i][j] = subMatrix[i][j] - subM_change[j];
						codeMatrix_temp[i][j] = codeCost(
								rowSet.get(max_Shannon) - number,
								colSet.get(j), subMatrix_temp[i][j]);
						Cost_temp += codeMatrix_temp[i][j];

					}
				}

				else {
					for (j = 0; j < l; j++) {
						subMatrix_temp[i][j] = subM_change[j];
						codeMatrix_temp[i][j] = codeCost(number, colSet.get(j),
								subMatrix_temp[i][j]);
						Cost_temp += codeMatrix_temp[i][j];

					}
				}
			}

			/* add descriptCost */
			new_cost = Cost_temp
					+ descriptCost(k, l, Set_temp, colSet, subMatrix_temp);

			rowSet = null;
			rowSet = Set_temp;

		}
		/* Column Iteration is same as above */
		else {

			if (number < n * col_size && setSize)
				return false;

			l++;

			subMatrix_temp = new long[k][l];
			codeMatrix_temp = new double[k][l];

			ArrayList<Long> Set_temp = new ArrayList<>();

			/* Update the subMatrix, codeMatrix */
			for (i = 0; i < l; i++) {
				if (i != l - 1 && i != max_Shannon)
					Set_temp.add(colSet.get(i));
				else if (i == max_Shannon)
					Set_temp.add(colSet.get(max_Shannon) - number);
				else
					Set_temp.add((long) number);
			}

			for (i = 0; i < k; i++) {
				for (j = 0; j < l; j++) {

					if (j != max_Shannon && j != l - 1) {
						subMatrix_temp[i][j] = subMatrix[i][j];
						codeMatrix_temp[i][j] = codeMatrix[i][j];
						Cost_temp += codeMatrix_temp[i][j];

					}

					else if (j == max_Shannon) {
						subMatrix_temp[i][j] = subMatrix[i][j] - subM_change[i];
						codeMatrix_temp[i][j] = codeCost(rowSet.get(i),
								colSet.get(max_Shannon) - number,
								subMatrix_temp[i][j]);
						Cost_temp += codeMatrix_temp[i][j];

					}

					else {
						subMatrix_temp[i][j] = subM_change[i];
						codeMatrix_temp[i][j] = codeCost(rowSet.get(i), number,
								subMatrix_temp[i][j]);
						Cost_temp += codeMatrix_temp[i][j];

					}
				}
			}

			new_cost = Cost_temp
					+ descriptCost(k, l, rowSet, Set_temp, subMatrix_temp);

			colSet = null;
			colSet = Set_temp;

		}

		subMatrix = null;
		subMatrix = subMatrix_temp;
		codeMatrix = null;
		codeMatrix = codeMatrix_temp;

		Cost = new_cost;
		cost_show.add(Cost);
		
		if(cur_job.equals("r"))
			nameChange(cur_job_output, row_nonzero, "assign", "nonzero");
		else
			nameChange(cur_job_output, col_nonzero, "assign", "nonzero");
		
		return true;

	}

	/**
	 * change permutation to diminish cost
	 * 
	 * @param first
	 *            : "r" or "c"
	 * @throws NumberFormatException
	 * @throws Exception
	 */
	public static void reGroup(String first) throws NumberFormatException,
			Exception {

		Job job;

		int iter;
		int key;

		boolean stop = false;

		StringTokenizer st1, st2;
		String cur_job = first;

		double temp = 0;
		Path cur_job_output;
		Path cur_input;
		Path row_result = new Path(outputPath + "/res-r");
		Path col_result = new Path(outputPath + "/res-c");

		/*
		 * Store Data from reducer result If the Cost is decreased, update data
		 * Otherwise discard the results
		 */
		ArrayList<Long> rowSet_temp = null;
		ArrayList<Long> colSet_temp = null;
		long[][] subMatrix_temp;
		double[][] codeMatrix_temp;

		while (!stop) {

			System.out.println("Regroup starts with " + cur_job);

			FileSystem fs = FileSystem.get(conf);
			/*
			 * Initialize Data Remove old data
			 */
			if (cur_job.equals("r")) {

				fs.delete(r_reg, true);
				fs.delete(row_result, true);

				cur_job_output = r_reg;
				cur_input = row_nonzero;

			} else {

				fs.delete(c_reg, true);
				fs.delete(col_result, true);

				cur_job_output = c_reg;
				cur_input = col_nonzero;

			}
			System.out.println("Sending Parameter " + cur_job);

			conf.setInt("l", l);
			conf.setInt("k", k);

			conf.set("rowSet", rowSet.toString());
			conf.set("colSet", colSet.toString());
			conf.set("subMatrix_String", matrixToString(subMatrix));

			conf.set("job", cur_job);

			job = new Job(conf);
			job.setJarByClass(CA.class);

			job.setJobName("reGroup" + " " + cur_job);

			FileInputFormat.addInputPath(job, cur_input);
			FileOutputFormat.setOutputPath(job, cur_job_output);

			MultipleOutputs.addNamedOutput(job, "subMatrix",
					TextOutputFormat.class, IntWritable.class, Text.class);
			MultipleOutputs.addNamedOutput(job, "assign",
					TextOutputFormat.class, IntWritable.class, Text.class);

			job.setMapperClass(Regroup_mapper.class);
			job.setReducerClass(Regroup_combiner.class);
			job.setCombinerClass(Regroup_reducer.class);
			job.setPartitionerClass(naive_partitioner_reg.class);

			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);

			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);

			job.setNumReduceTasks(num_machine);
			System.out.println("Start MapReduce " + cur_job);
			job.waitForCompletion(true);

			/* Recalculate subMatrix */
			subMatrix_temp = new long[k][l];
			Text read = new Text();
			temp = 0;

			/* Recalculate SubMatrices after row regrouping */
			if (cur_job.equals("r")) {

				rowSet_temp = new ArrayList<>();

				for (int i = 0; i < k; i++)
					rowSet_temp.add(0l);

				/* Read Data from MapReduce Results */
				for (LineReader lr : readLine(
						new Path(cur_job_output.toString() + "/subMatrix"), "s")) {

					while (lr.readLine(read) > 0) {

						if (read.getLength() == 0)
							continue;

						st1 = new StringTokenizer(read.toString(), "\t");
						key = Integer.parseInt(st1.nextToken());

						rowSet_temp.set(
								key,
								rowSet_temp.get(key)
										+ Long.parseLong(st1.nextToken()));

						iter = 0;
						st2 = new StringTokenizer(st1.nextToken(), " ");

						while (st2.hasMoreTokens())
							subMatrix_temp[key][iter++] += Long.parseLong(st2
									.nextToken());

						read.clear();

					}
				}

				/*
				 * Check for any zero cluster If there exists zero cluster, use
				 * del_zero_r to delete zero cluster
				 */
				int[] del_zero_r = new int[k];
				int i, j = 0;
				for (i = 0; i < k; i++) {
					if (rowSet_temp.get(i) != 0)
						del_zero_r[i] = j++;
					else
						del_zero_r[i] = -1;
				}

				codeMatrix_temp = new double[j][l];

				/* Case 1 : no zero cluster */
				if (j == k) {
					zero_cluster = false;

					/* Calculate Cost of Result */
					for (i = 0; i < k; i++) {
						for (j = 0; j < l; j++) {
							codeMatrix_temp[i][j] = codeCost(
									rowSet_temp.get(i), colSet.get(j),
									subMatrix_temp[i][j]);
							temp += codeMatrix_temp[i][j];
						}
					}

				}
				/* Case 2: zero cluster exists */
				else {
					zero_cluster = true;
					conf.set("del_zero", arrToString(del_zero_r));

					/* remove zero cluster */
					rowSet_temp = (ArrayList<Long>) rowSet_temp.stream()
							.filter((s) -> (s != 0))
							.collect(Collectors.toList());

					long[][] subMatrix_temp_2 = new long[j][l];

					int index;

					/*
					 * Calculate Cost of Result & remove zero cluster from
					 * submatrix
					 */
					for (i = 0; i < k; i++) {
						index = del_zero_r[i];
						for (j = 0; j < l; j++) {

							if (index < 0)
								continue;

							subMatrix_temp_2[index][j] = subMatrix_temp[i][j];

							codeMatrix_temp[index][j] = codeCost(
									rowSet_temp.get(index), colSet.get(j),
									subMatrix_temp_2[index][j]);
							temp += codeMatrix_temp[index][j];
						}
					}
					subMatrix_temp = subMatrix_temp_2;

				}

				temp += descriptCost(rowSet_temp.size(), l, rowSet_temp,
						colSet, subMatrix_temp);

				/* If the Result Does not reduce the Cost, End ReGroup */
				if (temp >= Cost)
					stop = true;

				/* Cost is decreased. Change Jobname and re-do ReGroup */
				else {
					rowSet.clear();
					rowSet = rowSet_temp;
					k = rowSet_temp.size();
					Cost = temp;
					codeMatrix = codeMatrix_temp;
					subMatrix = subMatrix_temp;

				}

				System.out
						.println("current row cluster : " + rowSet.toString());

			}

			/* Column iteration is same as above */
			else {

				colSet_temp = new ArrayList<>();

				for (int i = 0; i < l; i++)
					colSet_temp.add(0l);

				/* Read Data from MapReduce Results */
				for (LineReader lr : readLine(
						new Path(cur_job_output.toString() + "/subMatrix"), "s")) {

					while (lr.readLine(read) > 0) {

						if (read.getLength() == 0)
							continue;

						st1 = new StringTokenizer(read.toString(), "\t");
						key = Integer.parseInt(st1.nextToken());

						colSet_temp.set(key, Long.parseLong(st1.nextToken()));

						iter = 0;
						st2 = new StringTokenizer(st1.nextToken(), " ");

						while (st2.hasMoreTokens())
							subMatrix_temp[iter++][key] += Long.parseLong(st2
									.nextToken());

						read.clear();

					}
				}

				int[] del_zero_c = new int[l];
				int i, j = 0;
				for (i = 0; i < l; i++) {
					if (colSet_temp.get(i) != 0)
						del_zero_c[i] = j++;
					else
						del_zero_c[i] = -1;
				}

				codeMatrix_temp = new double[k][j];

				if (j == l) {
					zero_cluster = false;

					for (j = 0; j < k; j++) {
						for (i = 0; i < l; i++) {
							codeMatrix_temp[j][i] = codeCost(rowSet.get(j),
									colSet_temp.get(i), subMatrix_temp[j][i]);
							temp += codeMatrix_temp[j][i];
						}
					}

				} else {

					zero_cluster = true;
					conf.set("del_zero", arrToString(del_zero_c));

					colSet_temp = (ArrayList<Long>) colSet_temp.stream()
							.filter((s) -> (s != 0))
							.collect(Collectors.toList());

					long[][] subMatrix_temp_2 = new long[k][j];

					int index;
					/* Calculate Cost of Result */
					for (i = 0; i < l; i++) {
						index = del_zero_c[i];

						for (j = 0; j < k; j++) {

							if (index < 0)
								continue;

							subMatrix_temp_2[j][index] = subMatrix_temp[j][i];

							codeMatrix_temp[j][index] = codeCost(rowSet.get(j),
									colSet_temp.get(index),
									subMatrix_temp_2[j][index]);
							temp += codeMatrix_temp[j][index];
						}
					}

					subMatrix_temp = subMatrix_temp_2;

				}

				temp += descriptCost(k, colSet_temp.size(), rowSet,
						colSet_temp, subMatrix_temp);

				if (temp >= Cost) {
					stop = true;
				}
				/*
				 * Cost is decreased. Change Jobname and re-do ReGroup
				 */
				else {
					colSet.clear();
					colSet = colSet_temp;
					l = colSet_temp.size();
					Cost = temp;
					codeMatrix = codeMatrix_temp;
					subMatrix = subMatrix_temp;

				}

				System.out.println("current column cluster : "
						+ colSet.toString());

			}

			conf.setBoolean("zero_cluster", zero_cluster);
			conf.setInt("k", k);
			conf.setInt("l", l);

			System.out.println("reconstruct");
			job = new Job(conf);
			job.setJarByClass(CA.class);

			job.setJobName("reGroup" + " " + cur_job);

			if (cur_job.equals("r"))
				MultipleInputs
						.addInputPath(job, row_adj, TextInputFormat.class);
			else
				MultipleInputs
						.addInputPath(job, col_adj, TextInputFormat.class);

			MultipleInputs.addInputPath(job, new Path(cur_job_output.toString()
					+ "/assign"), TextInputFormat.class);

			FileOutputFormat.setOutputPath(job,
					new Path(cur_job_output.toString() + "/temp"));

			MultipleOutputs.addNamedOutput(job, "temp", TextOutputFormat.class,
					Text.class, Text.class);
			MultipleOutputs.addNamedOutput(job, "nonzero",
					TextOutputFormat.class, Text.class, Text.class);

			job.setMapperClass(Regroup_calc_mapper.class);
			job.setReducerClass(Regroup_calc_reducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.setNumReduceTasks(num_machine);

			job.waitForCompletion(true);

			/* Calculate nonzeros */
			job = new Job(conf);
			job.setJarByClass(CA.class);

			job.setJobName("reGroup" + " " + cur_job);

			if (cur_job.equals("r"))
				MultipleInputs.addInputPath(job, col_nonzero,
						TextInputFormat.class);
			else
				MultipleInputs.addInputPath(job, row_nonzero,
						TextInputFormat.class);
			MultipleInputs.addInputPath(job, new Path(cur_job_output.toString()
					+ "/temp/temp"), TextInputFormat.class);

			FileOutputFormat.setOutputPath(job,
					new Path(cur_job_output.toString() + "/nonzero"));

			MultipleOutputs.addNamedOutput(job, "adj", TextOutputFormat.class,
					Text.class, Text.class);

			job.setMapperClass(makeNew_split_mapper.class);
			job.setCombinerClass(makeNew_split_comibner.class);
			job.setReducerClass(makeNew_split_reducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.setNumReduceTasks(num_machine);

			job.waitForCompletion(true);

			if (cur_job.equals("r")) {
				nameChange(new Path(cur_job_output.toString() + "/nonzero"),
						col_nonzero, "part", "nonzero");
				if (zero_cluster)
					nameChange(new Path(cur_job_output.toString()
							+ "/temp/nonzero"), row_nonzero, "nonzero",
							"nonzero");
				else
					nameChange(new Path(cur_job_output.toString() + "/assign"),
							row_nonzero, "assign", "nonzero");
			} else {
				nameChange(new Path(cur_job_output.toString() + "/nonzero"),
						row_nonzero, "part", "nonzero");
				if (zero_cluster)
					nameChange(new Path(cur_job_output.toString()
							+ "/temp/nonzero"), col_nonzero, "nonzero",
							"nonzero");
				else
					nameChange(new Path(cur_job_output.toString() + "/assign"),
							col_nonzero, "assign", "nonzero");
			}

			cur_job = cur_job.equals("r") ? "c" : "r";

			cost_show.add(Cost);

		}

	}

	/**
	 * Usage : inputPath outputPath Options : -set x y : set maximum number of
	 * row, column clusters -size p q : set minimum size of row, column clusters
	 * by setting each cluster size > value/ cluster Size
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		conf = new Configuration();
		fs = FileSystem.get(conf);

		inputFile = args[0];
		outputPath = args[1];

		int max_k = 0;
		int max_l = 0;

		data = false;
		mode = false;
		setSize = false;

		row_nonzero = new Path(outputPath + "/preProcess/r/nonzero");
		row_adj = new Path(outputPath + "/preProcess/r/adj");

		col_nonzero = new Path(outputPath + "/preProcess/c/nonzero");
		col_adj = new Path(outputPath + "/preProcess/c/adj");
		r_inc = new Path(outputPath + "/incD-r");
		c_inc = new Path(outputPath + "/incD-c");

		r_reg = new Path(outputPath + "/reg-r");
		c_reg = new Path(outputPath + "/reg-c");

		m = Integer.parseInt(args[2]);
		n = Integer.parseInt(args[3]);
		conf.setInt("m", m);
		conf.setInt("n", n);

		if (args.length > 4) {
			int i = 4;
			while (i < args.length) {

				if (args[i].equals("-set")) {
					mode = true;
					max_k = Integer.parseInt(args[++i]);
					max_l = Integer.parseInt(args[++i]);
				}

				else if (args[i].equals("-size")) {
					setSize = true;
					row_size = Double.parseDouble(args[++i]);
					col_size = Double.parseDouble(args[++i]);
					if (row_size >= 1 || col_size >= 1)
						throw new Exception();
				}

				else if (args[i].equals("-data")) {
					data = true;
					inputFile = outputPath;
				}

				else if (args[i].equals("-machine")) {
					num_machine = Integer.parseInt(args[++i]);
				}

				else if (args[i].equals("-rand"))
					random = true;

				i++;
			}
		}

		/*
		 * } catch (Exception e) { System.out .println(
		 * "Usage: inputPath outputPath m n -set int int -size double double");
		 * System.out.println("m, n : size of dimension");
		 * System.out.println("-set a b : set Max number of clusters");
		 * System.out .println(
		 * "-size p q : set minumum portion of (each cluster size / whole size)\nnumber should be lower than 1"
		 * ); System.out.println("-data : use existing adjacency lists");
		 * return; }
		 */

		long total_weight = 0;

		long startTime = System.currentTimeMillis();

		if (!data)
			makeAdjList();

		long data_process_time = System.currentTimeMillis();

		ContentSummary cSummary = fs.getContentSummary(new Path(args[0]));
		long length = cSummary.getLength();
		fw = new PrintWriter(new OutputStreamWriter(fs.create(new Path(
				outputPath.toString() + "/result.txt"))));
		/*
		 * ` Initialize Variables m:row dimension n:col dimension k:row
		 * permutation dimension l:col permutation dimension t:Iteration number;
		 * 
		 * Initialize Structures row_permut: {1,2,3,...,m}->{1,2,3,4...,k}
		 * col_permut: {1,2,.....,n}->{1,2,......,l} rowSet,colSet : size of
		 * each permutation subMatrix: matrix of weight of each cluster
		 * codeMatrix: Code_cost Matrix for each cluster Cost:Current Code_Cost
		 * of given permutation
		 */

		/* Initialize Variables */

		k = 1;
		l = 1;
		int i, j;

		try {

			FileStatus[] fileStatus = fs.listStatus(new Path(outputPath
					+ "/preProcess/sum/"));

			Path[] paths = FileUtil.stat2Paths(fileStatus);

			for (Path path : paths) {
				String[] name = path.toString().split("/");
				if (name[name.length - 1].startsWith("p")) {
					total_weight += Long.parseLong(readLines(path).get(0)
							.split("\t")[1]);
				}
			}

		} catch (Exception e) {
			System.out.println("tried to calc" + total_weight);
		}

		fw.printf("File name:%s\nFile size : %,d bytes\n\n",
				inputFile.toString(), length);

		for (String s : args) {
			fw.print(s + " ");
		}

		fw.printf(
				"\nInitial Size : %,d * %,d\nInitial NonZeros: %,d\nInitial Density: %4f\n\n",
				m, n, total_weight, (double) total_weight / ((long) m * n));

		/* Initialize permutation set */
		rowSet = new ArrayList<>();
		colSet = new ArrayList<>();

		rowSet.add((long) m);
		colSet.add((long) n);

		/* Initialize Matrix to compute Code_Cost */
		codeMatrix = new double[1][1];
		subMatrix = new long[1][1];

		Cost = codeCost(m, n, total_weight);

		codeMatrix[0][0] = Cost;
		subMatrix[0][0] = total_weight;

		boolean row_permut_changed = true;
		boolean col_permut_changed = true;

		Cost += descriptCost(k, l, rowSet, colSet, subMatrix);
		cost_show.add(Cost);
		/*
		 * Outer Loop Starts Iterate until row permutation, column permutation
		 * become unchanged or code_Cost does not change
		 */

		int iter = 0;

		double tempCost = Double.MAX_VALUE;

		while (tempCost > Cost && (iter < 30)
				&& (row_permut_changed || col_permut_changed)) {

			iter++;

			tempCost = Cost;

			if (mode && max_k >= k && max_l >= l) {

				/******** row Iteration ********/

				/* Increase Row Dimension */
				if (max_k > k) {

					row_permut_changed = inc_Dimension("r");

					if (iter == 1 && !row_permut_changed) {
						random = true;
						continue;
					}
					/*
					 * If the row dimension increased, Start Inner Loop with row
					 * iteration
					 */
					if (row_permut_changed)
						reGroup("r");

				}

				/******** column Iteration ******/

				/* Increase Column Dimension */
				if (max_l > l) {

					col_permut_changed = inc_Dimension("c");

					/*
					 * If the column dimension increased, Start Inner Loop with
					 * column iteration
					 */
					if (col_permut_changed)
						reGroup("c");

				}

			} else {
				/******** row Iteration ********/

				/* Increase Row Dimension */
				row_permut_changed = inc_Dimension("r");

				if (iter == 1 && !row_permut_changed) {
					random = true;
					continue;
				}

				/*
				 * If the row dimension increased, Start Inner Loop with row
				 * iteration
				 */
				if (row_permut_changed)
					reGroup("r");

				/******** column Iteration ******/

				/* Increase Column Dimension */
				col_permut_changed = inc_Dimension("c");

				/*
				 * If the column dimension increased, Start Inner Loop with
				 * column iteration
				 */
				if (col_permut_changed)
					reGroup("c");

			}

		}

		long endTime = System.currentTimeMillis();

		/* Make final output */

		if (random)
			fw.println("Random Split is used");

		fw.printf(
				"time elpased: %,d msecs\nMake Adjacency Lists: %,d msecs\nBuild Clusters: %,d msecs.\n\n",
				endTime - startTime, data_process_time - startTime, endTime
						- data_process_time);

		fw.printf("rowSet:\nSize:%4d\n%s\n\n", rowSet.size(), rowSet.toString());
		fw.printf("colSet:\nSize:%4d\n%s\n\n", colSet.size(), colSet.toString());
		fw.println();

		fw.println("Size of Cluster");
		for (i = 0; i < k; i++) {
			for (j = 0; j < l; j++) {
				String s = String.format("[%,6d]",
						rowSet.get(i) * colSet.get(j));
				fw.print(s);
			}
			fw.println();
		}
		fw.println();

		fw.println("NonZeros of each Cluster");
		for (i = 0; i < k; i++) {
			for (j = 0; j < l; j++) {
				String s = String.format("[%,6d]", subMatrix[i][j]);
				fw.print(s);
			}
			fw.println();
		}
		fw.println();

		fw.println("NonZeros/Size");
		for (i = 0; i < k; i++) {
			for (j = 0; j < l; j++) {
				double x = (double) subMatrix[i][j]
						/ (rowSet.get(i) * colSet.get(j) + 1);
				if (x > 0.00001)
					fw.print(String.format("[%.3f]", x));
				else {
					fw.print(String.format("[%5d]", 0));
				}
			}
			fw.println();
		}
		fw.println();
		fw.println("cost decreased as " + cost_show.toString());

		fw.close();

	}

	private static double codeCost(long m, long n, double total_weight) {
		// if (m == 0 || n == 0 || total_weight == 0 || total_weight == m * n)
		// return 0;
		double prob = (total_weight) / (m * n);

		return (m * n)
				* ((prob == 0 ? 0 : prob * Math.log(1 / prob)) + (1 - prob == 0 ? 0
						: (1 - prob) * Math.log(1 / (1 - prob))));
	}

	private static double log(double num) {
		if (num <= 0)
			return 0;

		return Math.log(num) / Math.log(2);
	}

	private static double descriptCost(int k, int l, ArrayList<Long> rowSet,
			ArrayList<Long> colSet, long[][] subMatrix) {
		double D = 0;

		D += (log(k) + log(log(k))) + (log(l) + log(log(l)));

		for (long s : rowSet) {
			D += Math.ceil(log(s) + log(log(s)));
		}
		for (long s : colSet) {
			D += Math.ceil(log(s) + log(log(s)));
		}

		for (int i = 0; i < subMatrix.length; i++) {
			for (int j = 0; j < subMatrix[0].length; j++) {
				D += Math.ceil(log(subMatrix[i][j] + 1));
			}
		}

		return D;
	}

	/*
	 * code from
	 * http://blog.matthewrathbone.com/2013/12/28/Reading-data-from-HDFS
	 * -even-if-it-is-compressed.html read file in HDFS line by line
	 */
	public static ArrayList<String> readLines(Path location) throws Exception {
		FileSystem fileSystem = FileSystem.get(location.toUri(), conf);
		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		FileStatus[] items = fileSystem.listStatus(location);
		if (items == null)
			return new ArrayList<String>();
		ArrayList<String> results = new ArrayList<String>();
		for (FileStatus item : items) {

			// ignoring files like _SUCCESS
			if (item.getPath().getName().startsWith("_")) {
				continue;
			}

			CompressionCodec codec = factory.getCodec(item.getPath());
			InputStream stream = null;

			// check if we have a compression codec we need to use
			if (codec != null) {
				stream = codec
						.createInputStream(fileSystem.open(item.getPath()));
			} else {
				stream = fileSystem.open(item.getPath());
			}

			StringWriter writer = new StringWriter();
			IOUtils.copy(stream, writer, "UTF-8");
			String raw = writer.toString();
			String[] resulting = raw.split("\n");
			for (String str : resulting) {
				results.add(str);
			}
		}
		return results;
	}

	public static ArrayList<LineReader> readLine(Path location, String start)
			throws IOException {
		FileSystem fileSystem = FileSystem.get(location.toUri(), conf);
		FileStatus[] items = fileSystem.listStatus(location);
		ArrayList<LineReader> readLines = new ArrayList<>();
		for (FileStatus item : items) {

			// ignoring files like _SUCCESS
			if (item.getPath().getName().startsWith("_")
					|| !item.getPath().getName().startsWith(start)) {
				continue;
			} else {
				readLines.add(new LineReader(fileSystem.open(item.getPath())));
			}
		}
		return readLines;
	}

	public static ArrayList<LineReader> readLine(Path location)
			throws IOException {
		FileSystem fileSystem = FileSystem.get(location.toUri(), conf);

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

	public static void nameChange(Path fromPath, Path toPath, String fromName,
			String toName) throws IOException {
		FileStatus[] items = fs.listStatus(toPath);
		for (FileStatus item : items) {
			if (item.getPath().getName().startsWith(toName)) {
				fs.delete(item.getPath(), true);
			}
		}

		items = fs.listStatus(fromPath);
		String name;
		String[] split;

		for (FileStatus item : items) {
			if (item.getPath().getName().startsWith(fromName)) {
				name = "";
				split = item.getPath().toString().split("/");

				name += toName + "-r-" + split[split.length - 1].split("-")[2];
				fs.rename(item.getPath(), new Path(toPath.toString() + "/"
						+ name));
			}
		}
	}

	public static String matrixToString(double[][] matrix) {
		String ret = "";
		for (double[] line : matrix) {
			ret += "{";
			for (double d : line) {
				ret += d + ",";
			}
			ret += "}";
		}
		return ret;
	}

	public static String matrixToString(int[][] matrix) {
		String ret = "";
		for (int[] line : matrix) {
			ret += "{";
			for (double d : line) {
				ret += d + ",";
			}
			ret += "}";
		}
		return ret;
	}

	public static String matrixToString(long[][] matrix) {
		String ret = "";
		for (long[] line : matrix) {
			ret += "{";
			for (long d : line) {
				ret += d + ",";
			}
			ret += "}";
		}
		return ret;
	}

	private static String arrToString(int[] arr) {
		String ret = "";
		for (int d : arr) {
			ret += d + " ";
		}
		return ret;
	}

}
