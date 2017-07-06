package Client_Server;

import java.io.FileReader;
import java.io.IOException;
import java.net.Socket;
import java.util.Collection;
import java.util.Properties;

import com.briup.util.BIDR;
import com.briup.util.BackUP;
import com.briup.util.Logger;
import com.briup.woss.client.Client;
import com.briup.woss.client.Gather;

import WossConfiguration.WossConfiguration;

public class ClientStart {
	
	public static void main(String[] args) throws Exception {
		
		Properties p = new Properties();
		try {
			p.load(new FileReader("conf/conf.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		Collection<BIDR> list = null;
		WossConfiguration c = new WossConfiguration();
		c.init(p);
		
		Logger logger = c.getLogger();
		logger.info("客户端启动...");
		logger.info("采集模块启动...");
		
		Gather gather = c.getGather();
		logger.info("客户端模块启动...");
		
		BackUP backup = c.getBackup();
		Client client = c.getClient();
		
		try {
			while(true)
			{
				Collection<BIDR> l = null;
				try {
					l = (Collection<BIDR>) backup.load("key", true);
				} catch (Exception e) {
					break;
				}
				if(l!=null)
				client.send(l);
			}
			logger.info("数据采集中...");
			 list = gather.gather();
			logger.info("发送数据...");
			client.send(list);
			logger.info("数据发送完毕！！！");
		} catch (Exception e) {
			logger.info("发送失败，备份中...");
			backup.store("key", list, true);
			logger.info("备份完毕");
		}
	}
}
