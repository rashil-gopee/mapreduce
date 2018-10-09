package au.rmit.bde;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RatingCalculatorJobConf extends Configured implements Tool {

	
	//Map Class
	   static public class RatingCalculatorMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	   
	      @Override
	      protected void map(LongWritable offset, Text text, Context context) throws IOException, InterruptedException {
	      
	    	  
	    	  if (offset.get() != 0) {
			  String[] review = text.toString().split("\t");

			  String productId = "none";
			  int rating;

			  productId = review[3];
			  rating = Integer.parseInt(review[7]);

			  context.write(new Text(productId), new LongWritable(rating));
	    	  }
	    	  else
	    		  return;

	      }
	      
	   }

	   //Reducer
	   static public class RatingCalculatorReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	      private LongWritable averageRating = new LongWritable();

	      @Override
	      protected void reduce(Text token, Iterable<LongWritable> ratings, Context context)
	            throws IOException, InterruptedException {	    	  
	    	  
	         long counter = 0;
	         long totalRating = 0;
	         
	         //Calculate average of ratings
	         for (LongWritable rating : ratings) {
	        	 counter++;
	        	 totalRating += rating.get();
	         }
	         averageRating.set(totalRating/counter);
	 
	      
	         context.write(token, averageRating);
	      }
	   }
	   
//	   public static class RatingCalculatorPartioner extends Partitioner<Text, LongWritable>{
//			public int getPartition(Text key, LongWritable value, int numReduceTasks){
//			if(numReduceTasks==0)
//				return 0;
//			if(key.equals(new Text("Cricket")) && !value.equals(new Text("India")))
//				return 0;
//			if(key.equals(new Text("Cricket")) && value.equals(new Text("India")))
//				return 1;
//			else
//				return 2;
//			}
//		}

	   public int run(String[] args) throws Exception {
	      Configuration configuration = getConf();
//	      configuration.set("mapreduce.job.jar", "/home/hadoop/wordcount.jar");

	      configuration.set("mapreduce.job.jar", args[2]);
	      //Initialising Map Reduce Job
	      Job job = new Job(configuration, "Word Count");
	      
	      //Set Map Reduce main jobconf class
	      job.setJarByClass(RatingCalculatorMapper.class);
	      
	      //Set Mapper class
	      job.setMapperClass(RatingCalculatorMapper.class);
	      
	     //Set Combiner class
	      job.setCombinerClass(RatingCalculatorReducer.class);
	      
	      //set Reducer class
	      job.setReducerClass(RatingCalculatorReducer.class);
	      

	      //set Input Format
	      job.setInputFormatClass(TextInputFormat.class);
	      
	      //set Output Format
	      job.setOutputFormatClass(TextOutputFormat.class);

	      //set Output key class
	      job.setOutputKeyClass(Text.class);
	      
	      //set Output value class
	      job.setOutputValueClass(LongWritable.class);

	      FileInputFormat.addInputPath(job, new Path(args[0]));
	      FileOutputFormat.setOutputPath(job, new Path(args [1]));



	      return job.waitForCompletion(true) ? 0 : -1;
	   }

	   public static void main(String[] args) throws Exception {
	      System.exit(ToolRunner.run(new RatingCalculatorJobConf(), args));
	   }
	}
