package edu.indiana.soic.cs;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BasicStatisticsMR {

	public static double totalCountMapper = 0.0;

	public static double totalSumReducer = 0.0; // for average calculation
	public static double min = Double.MAX_VALUE;
	public static double max = Double.MIN_VALUE;

	// ---------------- MAP ---------------------------------------------------
	public static class Map extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {

		public HashMap<String, Double> mapIn = new HashMap<String, Double>(); // meant
																				// to
																				// be
																				// a
																				// local
																				// map

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString()); // line
																			// to
																			// string
																			// token
			while (itr.hasMoreTokens()) {
				String aLine = itr.nextToken();
				totalCountMapper++;
				if (mapIn.containsKey(aLine) == true) {
					double oldValue = mapIn.get(aLine);
					oldValue += Double.parseDouble(aLine);
					mapIn.put(aLine, oldValue);

				} else {
					double firstValue = Double.parseDouble(aLine);
					mapIn.put(aLine, firstValue);

				}

			}

		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {

			for (String FirstKey : mapIn.keySet()) {

				context.write(new Text(FirstKey),
						new DoubleWritable(mapIn.get(FirstKey)));
			}

			context.write(new Text("totalCount"), new DoubleWritable(
					totalCountMapper));
		}

	}

	// ---------------- REDUCE ------------------------------------------------
	public static class Reduce extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		public HashMap<String, Double> redIn = new HashMap<String, Double>(); // meant
																				// to
																				// be
																				// a
																				// local
																				// map

		private static double totalElements = 0.0;

		public void reduce(Text key, Iterable<DoubleWritable> valList,
				Context context) throws IOException, InterruptedException {

			for (DoubleWritable listItem : valList) {
				if (key.toString().equals("totalCount")) {
					totalElements += listItem.get();

				} else {

					if (Double.parseDouble(key.toString()) < min) {
						min = Double.parseDouble(key.toString());
					}

					if (Double.parseDouble(key.toString()) > max) {
						max = Double.parseDouble(key.toString());
					}

					if (redIn.containsKey(key.toString())) {
						double oldVal = redIn.get(key.toString())  +  listItem.get();
						redIn.put(key.toString(), oldVal);
					} else {
						redIn.put(key.toString(), listItem.get());
					}

					totalSumReducer += listItem.get();
				}
			}

		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {

			double avg = totalSumReducer / totalElements;
			double sumSquare = 0.0;

			context.write(new Text(" Average:"), new DoubleWritable(avg));
			context.write(new Text(" Min :"), new DoubleWritable(min));
			context.write(new Text(" Max:"), new DoubleWritable(max));

			// standard deviation calculation
			for (String value2 : redIn.keySet()) {

				double xi = Double.parseDouble(value2);
				double delta = (double) xi - avg;
				double kTimes = (double) (Math.round(redIn.get(value2) / xi));

				double deltaSquare = delta * delta;

				while (kTimes > 0.0) {
					sumSquare += deltaSquare;
					kTimes -= 1;
				}

			}
			double stdDev = Math.sqrt(sumSquare / totalElements);

			context.write(new Text(" Standard Deviation:"), new DoubleWritable(
					stdDev));
		}

	}

	// Driver program
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs(); // get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: BasicStatisticsMR <in> <out>");
			System.err.println("Found: ");
			for (String arg : otherArgs) {
				System.err.println("\t"+arg);
			}
			System.exit(2);
		}

		// create a job with name "BasicStatisticsMR"
		Job job = new Job(conf, "BasicStatisticsMR");
		job.setJarByClass(BasicStatisticsMR.class);
		job.setMapperClass(Map.class);
		// Add a combiner here, not required to successfully run the program
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
