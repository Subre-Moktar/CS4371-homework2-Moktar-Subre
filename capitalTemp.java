import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class capitalTemp {
	public static class Map_temp 
	extends Mapper<LongWritable, Text, Text, Text>{
	
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
	
	//Finds Each Word
	public void map(LongWritable Key, Text value, Context context)
		throws IOException, InterruptedException {
			
			String[] mydata = value.toString().split(",");
			String country = mydata[1];
			String city = mydata[3];
			String temp = mydata[7];
			
			if (checkNum(temp) & temp.contentEquals("-99") != true) {
					context.write(new Text(country + "," + city), new Text(temp));
					}
				}
				
		}

	public static class Map_capital 
		extends Mapper<LongWritable, Text, Text, Text>{
	
	
	//Finds Each Word
	public void map(LongWritable Key, Text value, Context context)
		throws IOException, InterruptedException {
			
			String[] mydata = value.toString().split(",");
			String country = mydata[0].replaceAll("\"", "");
			String capital = mydata[1].replaceAll("\"", "");
	
			context.write(new Text(country + "," + capital), new Text("True"));
			}
	}
				


	// Reduce
	public static class Reduce 
		extends Reducer<Text, Text, Text, FloatWritable>{
	
		
		private FloatWritable result = new FloatWritable();
		
		
		public void reduce(Text key, Iterable<Text> values, 
				Context context) throws IOException, InterruptedException{
			List<Float> hold = new ArrayList<Float>();
			Boolean check1 = false;
			for (Text val: values) {
				if (val.toString().contentEquals("True")) {
					check1 = true;
				}
				else {
					hold.add(Float.parseFloat(val.toString()));
				}
				
			}
			if (check1 == true) {
				int count = 0;
				float sum = 0;
				//adds all counts
				if (hold.isEmpty()) {
				}
				else {
					for (Float val : hold) {
						sum += val;
						count++;
						}
					//writes the sum and word together
					result.set(sum/count);
					context.write(key, result);
					}
				}
			}
		
		}
	
	//Starts the class
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		// The job to run hadoop
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "capitalTemp");
		job.setJarByClass(capitalTemp.class);
		
		//job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
				Map_temp.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class,
				Map_capital.class);
		
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
