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
	   
		  final private static LongWritable ONE = new LongWritable(1);
	      private Text tokenValue = new Text();

	      @Override
	      protected void map(LongWritable offset, Text text, Context context) throws IOException, InterruptedException {
	       
	    	  //Split line into tokens
//	    	  for (String token : text.toString().split("\\s+")) {
//	            tokenValue.set(token);
//
//
//	            context.write(tokenValue, ONE);
//	         }

			  String[] review = text.toString().split("\t");

			  String productId = "none";

//			  if(user.length >= 2) {
//				  productId = user[1];
//			  }

			  productId = review[3];

			  context.write(new Text(productId), ONE);

	      }
	   }

	   //Reducer
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
