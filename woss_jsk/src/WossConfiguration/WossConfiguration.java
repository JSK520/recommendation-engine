package WossConfiguration;

import java.io.IOException;
import java.util.Properties;

import com.briup.util.BackUP;
import com.briup.util.Configuration;
import com.briup.util.Logger;
import com.briup.woss.ConfigurationAWare;
import com.briup.woss.WossModule;
import com.briup.woss.client.Client;
import com.briup.woss.client.Gather;
import com.briup.woss.server.DBStore;
import com.briup.woss.server.Server;

import WossBackUP.WossBackUP;
import WossClient.WossClient;
import WossDBStore.WossDBStore;
import WossGather.WossGather;
import WossLogger.WossLogger;
import WossServer.WossServer;

public class WossConfiguration implements Configuration {

	private Properties p;
	private Logger logger;
	
	public void init(Properties p)
	{
		if(p==null)
		{
			try {
				p = new Properties();
				p.load(WossConfiguration.class.getResourceAsStream("conf.properties"));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		this.p = p;
	}
	
	//因为下面的对象都继承了这两个类，所以可以有接口
//	interface Woss extends WossModule,ConfigurationAWare{};
	
	private Object getWoss(String str) throws Exception
	{
			Class<?> c = Class.forName(str);
//			woss.init(p);
//			woss.setConfiguration(this);
			return  c.newInstance();
	}
	
	public BackUP getBackup() throws Exception {
		WossBackUP bp = (WossBackUP) getWoss(p.getProperty("BackUP"));
		bp.init(p);
		bp.setConfiguration(this);
		return  bp;
	}

	public Client getClient() throws Exception {
		WossClient c = (WossClient) getWoss(p.getProperty("Client"));
		c.init(p);
		c.setConfiguration(this);
		return  c;
	}

	public DBStore getDBStore() throws Exception {
		WossDBStore wd = (WossDBStore) getWoss(p.getProperty("DBStore"));
		wd.init(p);
		wd.setConfiguration(this);
		return  wd;
	}

	public Gather getGather() throws Exception {
		WossGather wg = (WossGather) getWoss(p.getProperty("Gather"));
		wg.init(p);
		wg.setConfiguration(this);
		return  wg;
	}

	public Logger getLogger() throws Exception {
		WossLogger wl = (WossLogger) getWoss(p.getProperty("Logger"));
		wl.init(p);
		wl.setConfiguration(this);
		return  wl;
	}
	
	public Server getServer() throws Exception {
		WossServer ws = (WossServer) getWoss(p.getProperty("Server"));
		ws.init(p);
		ws.setConfiguration(this);
		return  ws;
	}

}
