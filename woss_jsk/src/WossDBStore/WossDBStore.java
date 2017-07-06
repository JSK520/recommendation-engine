package WossDBStore;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Properties;

import com.briup.util.BIDR;
import com.briup.util.Configuration;
import com.briup.woss.ConfigurationAWare;
import com.briup.woss.server.DBStore;

public class WossDBStore implements DBStore, ConfigurationAWare {

	private Collection<BIDR> collection;
	private Properties p;
	private Connection connection;
	private Configuration conf;
	
	public void init(Properties p) {
		
		if(p==null)
		{
		try {
			p = new Properties();
			p.load(WossDBStore.class.getResourceAsStream("DBStore.properties"));
			
		} catch (Exception e) {
		}}
		try {
			Class.forName("com.mysql.jdbc.Driver");
			connection = DriverManager.getConnection(p.getProperty("url")+p.getProperty("database"),p.getProperty("user"),p.getProperty("password"));
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		this.p = p;
		
	}

	public void setConfiguration(Configuration conf) {
		this.conf = conf;
	}

	public void saveToDB(Collection<BIDR> list) throws Exception {
		String sql = "insert into t_detail values(?,?,?,?,?,?)";
		PreparedStatement ps = connection.prepareStatement(sql);
		
		for(BIDR i : list)
		{
			ps.setString(1, i.getAAA_login_name());
			ps.setString(2, i.getLogin_ip());
			ps.setTimestamp(3, i.getLogin_date());
			ps.setTimestamp(4, i.getLogout_date());
			ps.setString(5, i.getNAS_ip());
			ps.setInt(6, i.getTime_deration());
			
			ps.execute();
		}		
	}

}
