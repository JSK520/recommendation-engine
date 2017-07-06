package WossBackUP;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Properties;

import com.briup.util.BackUP;
import com.briup.util.Configuration;
import com.briup.woss.ConfigurationAWare;

import WossConfiguration.WossConfiguration;

public class WossBackUP implements BackUP,ConfigurationAWare{

	private Properties p;
	private Configuration conf;
	private ObjectInputStream ois;
	
	public void init(Properties p) {
		if(p==null)
		{
			try {
				p = new Properties();
				p.load(WossBackUP.class.getResourceAsStream("conf.properties"));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		this.p = p;
	}

	public void setConfiguration(Configuration conf) {
		this.conf = conf;
	}

	public Object load(String key, boolean flag) throws Exception {
		if(ois==null)
		{
			ois = new ObjectInputStream(new FileInputStream(p.getProperty(key)));
		}
		Object readObject = ois.readObject();
		return readObject;
	}

	public void store(String key, Object data, boolean flag) throws Exception {
		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(p.getProperty(key),flag));
		oos.writeObject(data);
		oos.flush();
		oos.close();
	}
}











