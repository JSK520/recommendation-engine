package com.jsk.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Job1 extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Job1(), args);
	}
	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		String inpath = conf.get("inpath");
		String outpath = conf.get("outpath");
		//构建job
		Job job = Job.getInstance(conf);
		
		//指定一个完整MapRed所在类以及相关信息
		job.setJarByClass(Job1.class);
		job.setJobName("Job1");
		//指定Map程序所在的类以及keyout valueout类型
		job.setMapperClass(VectorMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		//指定reduce程序所在的类以及keyout valueout类型IntWritable
		job.setReducerClass(VectorReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(List.class);
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

	
	static class VectorMapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] split = StringUtils.split(value.toString(), ' ');
			//10001 20001 1  变成向量矩阵 20001	[10001:1  10004:1  10005:1]
			context.write(new Text(split[1]), new Text(split[0]+":"+split[2]));
		}
	}
	
	static class VectorReducer  extends Reducer<Text, Text, Text, List<Text>>{
		@Override
		protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, List<Text>>.Context context)
				throws IOException, InterruptedException {
			
			List  list = new ArrayList<>();
			for (Text text : value) {
				list.add(text.toString());
			}
			context.write(key, list);
		}
	}
}


