package com.briup.chrasm.Start;

import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.Properties;

import com.briup.chrasm.Configuration.WossConfiguration;
import com.briup.util.BIDR;
import com.briup.util.Configuration;
import com.briup.util.Logger;
import com.briup.woss.server.DBStore;
import com.briup.woss.server.Server;

public class WossServerStart {

	public static void main(String[] args) throws Exception {
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
		logger.info("服务端启动......");
		logger.info("服务端模块启动...");
		Server server = c.getServer();
		
		logger.info("入库模块启动...");
		
		
		logger.info("数据接收中...");
		while(true){
			logger.info("准备接收.....");
			Collection<BIDR> collection = server.revicer();
			logger.info("接收数据: "+collection.size());
			new BDStoreThread(collection,c).start();
		}
	}
	static class BDStoreThread extends Thread{
		private Collection<BIDR> collection;
		private Configuration c;
		
		public BDStoreThread(Collection<BIDR> collection, Configuration c) {
			super();
			this.collection = collection;
			this.c = c;
		}


		@Override
		public void run() {
			try {
				Logger logger = c.getLogger();
				logger.info("准备入库.......");
				DBStore dbStore = c.getDBStore();
				dbStore.saveToDB(collection);
				logger.info("入库完毕！");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

}
