# project1

import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class project1amain {
	
	public static void main(String[] args) throws Exception {
		
		Configuration config = new Configuration();
		Job job = new Job(config, "mapper1");
		job.setJarByClass(project1amain.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(project1a_map1.class);
		job.setReducerClass(project1a_reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path p1 =new Path(args[1]);
        FileOutputFormat.setOutputPath(job,p1);
        p1.getFileSystem(config).delete(p1,true);
		job.waitForCompletion(true);
	
		Configuration config1 = new Configuration();
		Job job1 = new Job(config1, "mapper2");
		job1.setJarByClass(project1amain.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		job1.setMapperClass(project1a_map2.class);
		job1.setReducerClass(project1a_reduce.class);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		Path p2 =new Path(args[2]);
        FileOutputFormat.setOutputPath(job1,p2);
        p2.getFileSystem(config1).delete(p2,true);	
		job1.waitForCompletion(true);
		
		Configuration config2 = new Configuration();
		Job job2 = new Job(config2, "mapper3");
		job2.setJarByClass(project1amain.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(IntWritable.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		job2.setMapperClass(project1a_map3.class);
		job2.setReducerClass(project1a_reduce.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		Path p3 =new Path(args[3]);
        FileOutputFormat.setOutputPath(job2,p3);
        p3.getFileSystem(config2).delete(p3,true);	
		job2.waitForCompletion(true);
		
		Configuration config3 = new Configuration();
		Job job3 = new Job(config3, "mapper4");
		job3.setJarByClass(project1amain.class);
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(IntWritable.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(IntWritable.class);
		job3.setMapperClass(project1a_map4.class);
		job3.setReducerClass(project1a_reduce.class);
		job3.setInputFormatClass(TextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job3, new Path(args[0]));
		Path p4 =new Path(args[4]);
        FileOutputFormat.setOutputPath(job3,p4);
        p4.getFileSystem(config3).delete(p4,true);	
		job3.waitForCompletion(true);
	
	
	
		
		
		
	}
}


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class project1a_map1 extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] a = value.toString().split(",");
		if(a.length>1)
		{
		context.write(new Text(a[14]), new IntWritable(1));
		}
	}
}




import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class project1a_map2 extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] a = value.toString().split(",");
		if(a.length>1)
		{
			if(a[14].equals("32"))
			{
		context.write(new Text(a[14]), new IntWritable(1));
			}
		}
	}
}




import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class project1a_map3 extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] a = value.toString().split(",");
		if(a.length>1)
		{
			if(a[5].toUpperCase().matches(".*(THEFT).*"))
				{
		context.write(new Text(a[11]), new IntWritable(1));
				}
		}
	}
}





import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class project1a_map4 extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] a = value.toString().split(",");
		if(a.length>1)
		{
			SimpleDateFormat sdf=new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");
			Date d1 = new Date();
			try{
				d1=sdf.parse(a[18]);
				String a1="10/01/2014 00:00:00 AM";
				String a2="11/01/2015 00:00:00 AM";
				Date d2 = sdf.parse(a1);
				Date d3 = sdf.parse(a2);
				if(d1.after(d2)&&d1.before(d3))
				{
					if(a[8].toUpperCase().matches(".*TRUE.*"))
					{
						context.write(new Text("TOTAL NUMBER OF ARREST"), new IntWritable(1));

					}
				}
			}
			catch (ParseException e) {
				e.printStackTrace();
				
			}
			
				
		}
	}





import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
public class project1a_reduce extends Reducer<Text,IntWritable,Text, IntWritable> {
	public void reduce(Text key, Iterable<IntWritable> values , Context context) throws IOException, InterruptedException
	{
		int count =0;
		for(IntWritable value : values)
		{
			count = count + value.get();
		}
		context.write(key, new IntWritable(count));
	}

}

