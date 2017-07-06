package com.briup.chrasm.Logger;

import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;

import com.briup.util.Configuration;
import com.briup.util.Logger;
import com.briup.woss.ConfigurationAWare;

public class WossLogger implements Logger, ConfigurationAWare {
	private Properties p;
	private Configuration conf;
	private org.apache.log4j.Logger logger;
	@Override
	public void init(Properties p) {
		if(p==null){
			p=new Properties();
			try {
				p.load(WossLogger.class.getResourceAsStream("log4j.properties"));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		this.p=p;
		PropertyConfigurator.configure(p);
		logger = org.apache.log4j.Logger.getRootLogger();
	}

	@Override
	public void setConfiguration(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public void debug(String message) {
		logger.debug(message);
	}

	@Override
	public void error(String message) {
		logger.error(message);
	}

	@Override
	public void fatal(String message) {
		logger.fatal(message);
	}

	@Override
	public void info(String message) {
		logger.info(message);
	}

	@Override
	public void warn(String message) {
		logger.warn(message);
	}

}
