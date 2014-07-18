/*
#
# Author: Gabriel Zhou
# Date: 2013/03/01
# 
*/
package Util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class ConfigManager {
	
private static Properties properties =  new Properties();
	
	static {
		try {
			properties.load(new FileInputStream(new File(KarmaAdaptor.SLOSHRun.config_path)));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static String getProperty(String key){
		return properties.getProperty(key);
	}
}
