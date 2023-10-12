import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class yearTemp {
	//Creation of map class
		public static class Map 
			extends Mapper<LongWritable, Text, Text, FloatWritable>{
			
			public static boolean checkNum(String check) {
				if (check == null) {
					return false;
				}
				try {
					float temp = Float.parseFloat(check); 
					}
					catch (NumberFormatException nfe) {
						return false;
					}
					return true;
				}
			
			
			//Each word gets count 1
		
		private Text word = new Text();
		
		//Finds Each Word
		public void map(LongWritable Key, Text value, Context context)
			throws IOException, InterruptedException {
				
				String[] mydata = value.toString().split(",");
				String region = mydata[0];
				String year = mydata[6];
				String temp = mydata[7];
				String country = mydata[1];
				
				
				if (checkNum(temp) & temp.contentEquals("-99") != true) {
					if (region.contentEquals("Asia")) {
						float num = Float.parseFloat(temp);
						
						context.write(new Text(country + ", " +year), new FloatWritable(num));
						}
					}
					
				}
			}
		
		// Reduce
		public static class Reduce 
			extends Reducer<Text, FloatWritable, Text, FloatWritable>{
		
			// adds to the count
		private FloatWritable result = new FloatWritable();
		
		public void reduce(Text key, Iterable<FloatWritable> values, 
				Context context) throws IOException, InterruptedException{
			int count = 0;
			float sum = 0;
			//adds all counts
			for (FloatWritable val : values) {
				sum += val.get();
				count++;
				}
			//writes the sum and word together
			result.set(sum/count);
			context.write(key, result);
			}
		
		}
		
		//Starts the class
		public static void main(String[] args) throws Exception {
			Configuration conf = new Configuration();
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			
			if (otherArgs.length != 2) {
				System.err.println("Usage: Word Count <in> <out>");
			}
			// The job to run hadoop
			@SuppressWarnings("deprecation")
			Job job = new Job(conf, "regionTemp");
			job.setJarByClass(yearTemp.class);
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			
			job.setOutputKeyClass(Text.class);
			
			job.setOutputValueClass(FloatWritable.class);
			
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
			
			System.exit(job.waitForCompletion(true) ? 0 : 1);
			
		}

}
