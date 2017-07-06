package com.briup.chrasm.Configuration;


//≈‰÷√ƒ£øÈ

import java.io.IOException;
import java.util.Properties;

import com.briup.chrasm.BackUP.WossBackUP;
import com.briup.chrasm.Client.WossClient;
import com.briup.chrasm.DBStore.WossDBStore;
import com.briup.chrasm.Gather.WossGather;
import com.briup.chrasm.Logger.WossLogger;
import com.briup.chrasm.Server.WossServer;
import com.briup.util.BackUP;
import com.briup.util.Configuration;
import com.briup.util.Logger;
import com.briup.woss.ConfigurationAWare;
import com.briup.woss.WossModule;
import com.briup.woss.client.Client;
import com.briup.woss.client.Gather;
import com.briup.woss.server.DBStore;
import com.briup.woss.server.Server;

public class WossConfiguration implements WossModule, Configuration {
	private Properties p;

	@Override
	public void init(Properties p) {
		if(p==null){
			p=new Properties();
			try {
				p.load(WossConfiguration.class.getResourceAsStream("conf.properties"));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		this.p=p;
	}
//	interface Woss extends WossModule,ConfigurationAWare{}
	private Object getWoss(String classname) throws Exception{
		Class<?>c = Class.forName(classname);
//		Woss woss = (Woss)c.newInstance();
//		woss.init(p);
//		woss.setConfiguration(this);
		return c.newInstance();
	}
	
	@Override
	public BackUP getBackup() throws Exception {
		WossBackUP wbup = (WossBackUP)getWoss(p.getProperty("BackUP"));
		wbup.init(p);
		wbup.setConfiguration(this);
		return wbup;
	}

	@Override
	public Client getClient() throws Exception {
		WossClient wc = (WossClient)getWoss(p.getProperty("Client"));
		wc.init(p);
		wc.setConfiguration(this);
		return wc;
	}

	@Override
	public DBStore getDBStore() throws Exception {
		WossDBStore wdbs = (WossDBStore)getWoss(p.getProperty("DBStore"));
		wdbs.init(p);
		wdbs.setConfiguration(this);
		return wdbs;
	}

	@Override
	public Gather getGather() throws Exception {
		WossGather wg = (WossGather)getWoss(p.getProperty("Gather"));
		wg.init(p);
		wg.setConfiguration(this);
		return wg;
	}

	@Override
	public Logger getLogger() throws Exception {
		WossLogger wl = (WossLogger)getWoss(p.getProperty("Logger"));
		wl.init(p);
		wl.setConfiguration(this);
		return wl;
	}

	@Override
	public Server getServer() throws Exception {
		WossServer ws = (WossServer)getWoss(p.getProperty("Server"));
		ws.init(p);
		ws.setConfiguration(this);
		return ws;
	}

	

}
