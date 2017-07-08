package com.jsk.job;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Job5 extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Job5(), args);
	}
	@Override
	public int run(String[] args) throws Exception {
		

		Configuration conf = getConf();
		String vinpath = conf.get("vinpath");
		String sinpath = conf.get("sinpath");
		String output = conf.get("outpath");

		Job job = Job.getInstance(conf);
		job.setJarByClass(this.getClass());
		job.setJobName("join user and artist");

		MultipleInputs.addInputPath(job, new Path(vinpath), TextInputFormat.class, Vector1Mapper.class);
		MultipleInputs.addInputPath(job, new Path(sinpath), TextInputFormat.class, Simary1Mapper.class);

		TextOutputFormat.setOutputPath(job, new Path(output));


		//判断一下输入的路径是否存在  存在就先删除
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(output)))
			fs.delete(new Path(output), true);
		
		job.setMapOutputKeyClass(Text.class);
		job.setReducerClass(MultiReducer.class);

		job.setOutputKeyClass(NullWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;
	
	}
	static class Vector1Mapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			String s = value.toString();
			
			String replace = s.replace("]", "").trim();
			String replace2 = replace.replace("[", "").trim();
			String str = replace2.substring(6, replace2.length()).trim();
			String key1 = replace2.substring(0, 5);
			
			String strs = "V, "+str;
			context.write(new Text(key1), new Text(strs));
		
		}
	}
	static class Simary1Mapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
String s = value.toString();
			
			String replace = s.replace("]", "").trim();
			String replace2 = replace.replace("[", "").trim();
			String str = replace2.substring(6, replace2.length()).trim();
			String key1 = replace2.substring(0, 5);
			String strs = "S, "+str;
			context.write(new Text(key1), new Text(strs));
			
		}
	}
	static class MultiReducer extends Reducer<Text, Text, Text, List<Text>>{
		@Override
		protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, List<Text>>.Context context)
				throws IOException, InterruptedException {
			List list = new ArrayList<>();
			for (Text text : value) {
				list.add(text);
			}
			
			
			context.write(key, list);
		}
	}
}

