package cn.com.pander.conf;

import java.io.InputStream;
import java.util.Properties;

public class ConfigurationManager {
	
	private static Properties prop = new Properties();
	
	//利用类加载的时候初始化静态方法
	static {
		try {
			InputStream in = ConfigurationManager.class
					.getClassLoader().getResourceAsStream("my.properties"); 
			prop.load(in);  
		} catch (Exception e) {
			e.printStackTrace();  
		}
	}
	/**
	 * 根据指定的key得到指定的value
	 * @param key
	 * @return
	 */
	public static String getProperty(String key) {
		return prop.getProperty(key);
	}
	
	/**
	 * 获取整数类型的配置
	 */
	public static Integer getInteger(String key) {
		String value = getProperty(key);
		try {
			return Integer.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}
	
	/**
	 * 获取boolean类型的参数
	 * @param key
	 * @return
	 */
	public static Boolean getBoolean(String key) {
		String value = getProperty(key);
		try {
			return Boolean.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * 获取long类型的配置
	 * @param key
	 * @return
	 */
	public static Long getLong(String key) {
		String value = getProperty(key);
		try {
			return Long.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0L;
	}

}
