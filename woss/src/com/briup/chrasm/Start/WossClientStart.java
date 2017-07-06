package com.briup.chrasm.Start;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.Properties;

import com.briup.chrasm.Configuration.WossConfiguration;
import com.briup.util.BIDR;
import com.briup.util.BackUP;
import com.briup.util.Configuration;
import com.briup.util.Logger;
import com.briup.woss.client.Client;
import com.briup.woss.client.Gather;

public class WossClientStart {
	public static void test() throws Exception{
		Properties p = new Properties();
		try {
			p.load(new FileReader("conf/conf.properties"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		WossConfiguration c = new WossConfiguration();
		c.init(p);
		Logger logger = c.getLogger();
		logger.info("客户端启动......");
		logger.info("采集模块加载中......");
		Gather gather = c.getGather();
		logger.info("客户端模块加载中......");
		Client client = c.getClient();
		logger.info("备份模块加载中......");
		BackUP backup = c.getBackup();
		
		logger.info("数据采集中......");
		Collection<BIDR> list = gather.gather();
		logger.info("采集数据："+list.size());
		try{
			logger.info("发送数据......");
			client.send(list);
			logger.info("数据发送完毕！！！！");
			
			
			logger.info("检查备份......");
			File file = new File(p.getProperty("databackup"));
			if(file.exists()){
				if(file.isDirectory()){
					String[] blist = file.list();
					for(String i:blist){
						String str = file.getName()+"/"+i;
						logger.info("备份文件："+str);
						Collection<BIDR> l = (Collection<BIDR>)backup.load(str, true);
						logger.info("备份数据："+l.size());
						logger.info("发送备份数据.....");
						client.send(l);
						logger.info("备份数据发送完毕！");
					}
				}
			}
			
		}catch(Exception e){
			logger.info("发送失败,备份中......");
			String filename = "databackup/"+System.currentTimeMillis();
			logger.info("备份文件："+filename);
			backup.store(filename, list, true);
			logger.info("备份完毕！");
		}
	}
	public static void main(String[] args) throws Exception {
		test();
//		test();
	}

}
