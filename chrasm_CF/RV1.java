package com.briup.chrasm_CF;

import java.io.IOException;

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


public class RV1 extends Configured implements Tool {

	static class rv11Mapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] datas = value.toString().split("[ ]");
			context.write(new Text(datas[0]), new Text("s"+datas[1]));
		}
	}
	static class rv12Mapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] datas = value.toString().split("[ ]");
			context.write(new Text(datas[0]), new Text("p"+datas[1]));
		}
	}
	static class rv1Reducer extends Reducer<Text, Text,Text, NullWritable> {
		
		private NullWritable v = NullWritable.get();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String[] s = null;
			String[] p = null;
			for(Text i:values){
				
				if(i.toString().startsWith("s")){
					s = i.toString().substring(1).split("[,]");
				}
				else p = i.toString().substring(1).split("[,]");
			}
			for(String x:s){
				for(String y:p){
					String[] xs = x.split("[:]");
					String[] ys = y.split("[:]");
					context.write(new Text(ys[0]+","+xs[0]+" "+(Integer.parseInt(xs[1])*Integer.parseInt(ys[1]))), v);
				}
			}
			
		}

	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Path input_sm = new Path(conf.get("input_sm"));
		Path input_pv = new Path(conf.get("input_pv"));
		Path output = new Path(conf.get("output"));

		Job job = Job.getInstance(conf);
		job.setJarByClass(this.getClass());
		job.setJobName("chrasm_CF");

		MultipleInputs.addInputPath(job, input_sm, TextInputFormat.class, rv11Mapper.class);
		MultipleInputs.addInputPath(job, input_pv, TextInputFormat.class, rv12Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setReducerClass(rv1Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		TextOutputFormat.setOutputPath(job, output);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new RV1(), args));
	}

}