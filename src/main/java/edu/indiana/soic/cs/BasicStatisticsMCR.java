package edu.indiana.soic.cs;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class BasicStatisticsMCR {

	// ---------------- MAP ---------------------------------------------------
	public static class Map extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {

		private final static DoubleWritable one = new DoubleWritable(1.00); // type of
																	// output
																	// value
		private Text wordKey = new Text(); // type of output key

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString()); // line
																			// to
																			// string
																			// token

			while (itr.hasMoreTokens()) {
				wordKey.set(itr.nextToken()); // set word as each input keyword
				context.write(wordKey, one); // create a pair <keyword, 1>
			}
		}

	}

	// ---------------- COMBINE -----------------------------------------------
	public static class combiner extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		private DoubleWritable result = new DoubleWritable();

		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0; // initialize the sum for each keyword
			for (DoubleWritable val : values) {
				sum += val.get();
			}
			result.set(sum);

			context.write(key, result); // create a pair <keyword, number of
										// occurences>
		}
	}

	// ---------------- REDUCE ------------------------------------------------
	public static class Reduce extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		private DoubleWritable sdResult = new DoubleWritable();
		private DoubleWritable avgResult = new DoubleWritable();
		private DoubleWritable minResult = new DoubleWritable();
		private DoubleWritable maxResult = new DoubleWritable();
		
		private ArrayList<Double> allArray = new ArrayList<Double>();
		
		private Text minText = new Text(">> Min : ");
		private Text maxText = new Text(">> Max : ");
		private Text averageText = new Text(">> Average : ");
		private Text sdText = new Text(">> Standard Deviation : ");
		
		private int count = 0;
		private Double doubleSum = 0.0;
		private Double min = Double.MAX_VALUE;
		private Double max = Double.MIN_VALUE;

		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0; // initialize the sum for each keyword
			Double keyValue = Double.parseDouble(key.toString());
			for (DoubleWritable val : values) {
				sum += val.get();

			}
			for (int i = 0; i < sum; i++) {
				allArray.add(keyValue);
			}

			count += sum;
			doubleSum = doubleSum + keyValue * sum;
			if (min > keyValue) {
				min = keyValue;
			}

			if (max < keyValue) {
				max = keyValue;
			}

		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {

			double average = doubleSum / count;
			maxResult.set(max);
			minResult.set(min);
			avgResult.set(average);
			double square = 0;
			for (double x : allArray) {
				square += Math.pow((average - x), 2);
			}
			square = square / count;
			square = Math.sqrt(square);
			sdResult.set(square);

			context.write(averageText, avgResult);
			context.write(minText, minResult);
			context.write(maxText, maxResult);
			context.write(sdText, sdResult);
		}

	}

	// Driver program
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs(); // get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: BasicStatisticsMCR <in> <out>");
			System.err.println("Found: ");
			for (String arg : otherArgs) {
				System.err.println("\t"+arg);
			}
			System.exit(2);
		}

		// create a job with name "BasicStatisticsMCR"
		Job job = new Job(conf, "BasicStatisticsMCR");
		job.setJarByClass(BasicStatisticsMCR.class);
		job.setMapperClass(Map.class);
		// Add a combiner here, not required to successfully run the program
		job.setCombinerClass(combiner.class);
		job.setReducerClass(Reduce.class);

		Timestamp currentTimestamp = new java.sql.Timestamp(Calendar
				.getInstance().getTime().getTime());
		String prefix = currentTimestamp.toString();
		String outputFolder = prefix.replace(".", "-").replace(" ", "-")
				.replace(":", "-")
				+ "-" + otherArgs[1];

		// set number of reduce tasks
		job.setNumReduceTasks(1);

		// set reducer output key type
		job.setOutputKeyClass(Text.class);

		// set reducer output value type
		job.setOutputValueClass(DoubleWritable.class);

		// set mapper output key type
		job.setMapOutputKeyClass(Text.class);

		// set mapper output value type
		job.setMapOutputValueClass(DoubleWritable.class);

		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(outputFolder));

		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
