package com.briup.chrasm_CF;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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


public class RV3 extends Configured implements Tool {

	static class rv31Mapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String k = value.toString().split("[,]")[0];
			context.write(new Text(k), new Text("r"+value.toString()));
		}
	}
	static class rv32Mapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[]data = value.toString().split("[ ]");
			context.write(new Text(data[0]), new Text("u"+data[0]+","+data[1]));
		}
	}
	static class rv3Reducer extends Reducer<Text, Text,Text, NullWritable> {
		
		private NullWritable v = NullWritable.get();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Map<String, String>r=new HashMap<>();
			List<String>u=new ArrayList<>();
			for(Text i:values){
				if(i.toString().startsWith("r")){
					String[] d = i.toString().substring(1).split("[ ]");
					r.put(d[0], d[1]);
				}else {
					u.add(i.toString().substring(1));
				}
				
			}
			for(String i:u){
				r.remove(i);
			}
			for(String i:r.keySet()){
				context.write(new Text(i+" "+r.get(i)), v);
			}
		}

	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Path input_rv = new Path(conf.get("input_rv"));
		Path input_uv = new Path(conf.get("input_uv"));
		Path output = new Path(conf.get("output"));

		Job job = Job.getInstance(conf);
		job.setJarByClass(this.getClass());
		job.setJobName("chrasm_CF");

		MultipleInputs.addInputPath(job, input_rv, TextInputFormat.class, rv31Mapper.class);
		MultipleInputs.addInputPath(job, input_uv, TextInputFormat.class, rv32Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setReducerClass(rv3Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		TextOutputFormat.setOutputPath(job, output);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new RV3(), args));
	}

}