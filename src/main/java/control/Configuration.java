package control;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Configuration {

	static {
		loadProperties();
	}
	
	private static Properties properties;
	
	// General
	private static String machineName;
	
	// Rest
	private static int serverPort;
	
	private static void loadProperties() {
		properties = new Properties();
		InputStream is = Configuration.class.getClassLoader().getResourceAsStream("config.properties");
		try {
			properties.load(is);
			serverPort = Integer.parseInt(properties.getProperty("serverPort", "8080"));
			machineName = properties.getProperty("machineName");
			// TODO 2: include check for mandatory fields
		} catch (IOException | NumberFormatException e) {
			System.err.println("Error while loading property file");
			e.printStackTrace();
		}
	}

	// ************************************************************
	// Generated Code
	// ************************************************************
	
	public static int getServerPort() {
		return serverPort;
	}

	public static void setServerPort(int serverPort) {
		Configuration.serverPort = serverPort;
	}

	public static String getMachineName() {
		return machineName;
	}

	public static void setMachineName(String machineName) {
		Configuration.machineName = machineName;
	}
	
}
