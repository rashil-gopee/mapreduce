package au.rmit.bde;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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
//import org.apache.log4j.Logger;

public class RatingCalculatorJobConf extends Configured implements Tool {
	//Map Class
	   static public class RatingCalculatorMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		final private static LongWritable ONE = new LongWritable(1);
//		   private Logger logger = Logger.getLogger(RatingCalculatorMapper.class);
		   //Simple Mapper
		   @Override
		      protected void map(LongWritable offset, Text text, Context context) throws IOException, InterruptedException {
		    	  if (offset.get() != 0) {
					  String[] review = text.toString().split("\t");
		
					  String productId = "none";
					  char confirmedPurchase;

					  productId = review[3];
					  confirmedPurchase = review[11].charAt(0);

					  if (confirmedPurchase == 'Y')
					  	context.write(new Text(productId), ONE);
		    	  }
		      }
		   
		   
//		   private Map<String, Float> tokenMap;
//		  
//		   
//		   @Override
//		    protected void setup(Context context) throws IOException, InterruptedException {
//		           tokenMap = new HashMap<String, Float>();
//		    }
//	   
//	       @Override
//		      protected void map(LongWritable offset, Text text, Context context) throws IOException, InterruptedException {
//	    	  if (offset.get() != 0) {
//				  String[] review = text.toString().split("\t");
//	
//				  String productId = "none";
//				  float rating;	
//				  
//				  productId = review[3];
//				  rating = Float.parseFloat(review[7]);
//	
//				  context.write(new Text(productId), new FloatWritable(rating));
//	    	  }
//	    	  else
//	    		  return;
//	      }
	   
	
	      
//	      @Override
//	      protected void cleanup(Context context) throws IOException, InterruptedException {
//	          IntWritable writableCount = new IntWritable();
//	          Text text = new Text();
//	          Set<String> keys = tokenMap.keySet();
//	          for (String s : keys) {
//	              text.set(s);
//	              writableCount.set(tokenMap.get(s));
//	              context.write(text,writableCount);
//	          }
//	      }
	      
	   }

	   static public class RatingCalculatorReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		   private LongWritable total = new LongWritable();

	      @Override
	      protected void reduce(Text token, Iterable<LongWritable> counts, Context context)
	            throws IOException, InterruptedException {

			  long n = 0;
			  //Calculate sum of counts
			  for (LongWritable count : counts)
				  n += count.get();
			  total.set(n);


			  context.write(token, total);

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
	      Job job = new Job(configuration, "Rating Average");
	      
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
