package com.briup.chrasm_CF;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class PC extends Configured implements Tool {

	static class pc1Mapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] datas = value.toString().split("[ ]");
			context.write(new Text(datas[0]), new Text("c"+datas[1]));
		}
	}
	static class pc2Mapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] datas = value.toString().split("[ ]")[0].split("[,]");
			context.write(new Text(datas[0]), new Text("r"+datas[1]));
		}
	}
	static class pcReducer extends Reducer<Text, Text,Text, NullWritable> {
		
		private NullWritable v = NullWritable.get();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			List<String>r=new ArrayList<>();
			List<String>c=new ArrayList<>();
			for(Text i:values){
				
				if(i.toString().startsWith("r")){
					r.add(i.toString().substring(1));
				}
				else c.add(i.toString().substring(1));
			}
			int n=0;
			for(String i:r){
				if(c.contains(i))n++;
			}
			String pstr=String.format("%.2f", (double)n/r.size());
			String cstr=String.format("%.2f", (double)n/c.size());
			context.write(new Text(key.toString()+" "+pstr+","+cstr), v);
		}

	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Path input_cs = new Path(conf.get("input_cs"));
		Path input_rv = new Path(conf.get("input_rv"));
		Path output = new Path(conf.get("output"));

		Job job = Job.getInstance(conf);
		job.setJarByClass(this.getClass());
		job.setJobName("chrasm_CF");

		MultipleInputs.addInputPath(job, input_cs, TextInputFormat.class, pc1Mapper.class);
		MultipleInputs.addInputPath(job, input_rv, TextInputFormat.class, pc2Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setReducerClass(pcReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		TextOutputFormat.setOutputPath(job, output);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new PC(), args));
	}

}