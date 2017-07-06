package Client_Server;

import java.io.FileReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collection;
import java.util.Properties;

import com.briup.util.BIDR;
import com.briup.util.Logger;
import com.briup.woss.server.DBStore;
import com.briup.woss.server.Server;

import WossConfiguration.WossConfiguration;


public class ServerStart {
	public static void main(String[] args) throws Exception {
		
		Properties p = new Properties();
		try {
			p.load(new FileReader("conf/conf.properties"));
		} catch (Exception e) {
		}
		
		WossConfiguration c = new WossConfiguration();
		c.init(p);
		
		Logger logger = c.getLogger();
		logger.info("服务端启动...");
		
		logger.info("服务端模块启动...");
		Server server = c.getServer();
		
		logger.info("入库模块启动...");
		DBStore dbStore = c.getDBStore();
		
		
		logger.info("数据接收中...");
		logger.info("数据入库中...");
		
		
		while(true)
		{
			Collection<BIDR> revicer = server.revicer();
//			System.out.println(revicer.size());
			new BDStoreThread(revicer,dbStore).start();;
			logger.info("数据入库成功");
		}
	}
	
	
		static class BDStoreThread extends Thread{
			private Collection<BIDR> collection;
			private DBStore dbStore;
			
			public BDStoreThread(Collection<BIDR> collection,DBStore dbStore)
			{ 
				this.collection = collection;
				this.dbStore = dbStore;
			}
			public void run()
			{
				try {
					dbStore.saveToDB(collection);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
}
