package com.jsk.job;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Job3 extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Job3(), args);
	}
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		String inpath = conf.get("inpath");
		String outpath = conf.get("outpath");
		//构建job
		Job job = Job.getInstance(conf);
		
		//指定一个完整MapRed所在类以及相关信息
		job.setJarByClass(Job3.class);
		job.setJobName("Job3");
		//指定Map程序所在的类以及keyout valueout类型
		job.setMapperClass(Simliary1Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		//指定reduce程序所在的类以及keyout valueout类型IntWritable
		job.setReducerClass(Similary1Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//判断一下输入的路径是否存在  存在就先删除
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(outpath)))
			fs.delete(new Path(outpath), true);
		
		//指定数据来源和数据去向相关的输入输出流以及文件位置
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(inpath));
		TextOutputFormat.setOutputPath(job, new Path(outpath));
		//提交运行任务
		
		return job.waitForCompletion(true)?0:1;
	}
	static class Simliary1Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String s = value.toString();
			
			String replace = s.replace("]", "").trim();
			String replace2 = replace.replace("[", "").trim();
			String str = replace2.substring(6, replace2.length()).trim();
			String[] strs = str.split(",");
			for (int i = 0; i < strs.length; i++) {
				for (int j = 0; j < strs.length; j++) {
					
					context.write(new Text(strs[i].trim()+"\t"+strs[j].trim()), new IntWritable(1));
				}
			}
		}
	}
	static class Similary1Reducer extends Reducer<Text, IntWritable, Text,IntWritable>{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> value, Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable i : value) {
				count += i.get();
			}
			context.write(key, new IntWritable(count));
		}
	}
}

