package KarmaClient;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

import edu.indiana.dsi.karma.client.messaging.Notification;
import edu.indiana.dsi.karma.messaging.MessageConfig;

import org.apache.log4j.Logger;

import Util.ConfigManager;

public class client {

	private String KARMA_PROPERTIES;
	private String notification_dir;
	private String query_dir;
	private String KARMA_HOME;

	/* Logging Mechanism */
	static final Logger logger = Logger.getLogger("KarmaClient.client");

	public client(String notification_folder) {
		ConfigManager config_manager = new ConfigManager();
		KARMA_PROPERTIES = config_manager.getProperty("Karma_Properties");
		notification_dir = notification_folder;
		query_dir = config_manager.getProperty("OPM_DIR");

		logger.info("Karma client is invoked...");
		logger.info("Loading KARMA PROPERTIES:" + KARMA_PROPERTIES);
		logger.info("Loading Karma notification path:" + notification_dir);
	}

	public void sendNotifications() {
		logger.info("Karma Client is sending notifications...");

		File dir = new File(notification_dir);
		if (new File(KARMA_PROPERTIES).exists()) {
			MessageConfig config = new MessageConfig(KARMA_PROPERTIES);
			Notification notificator = new Notification(config);
			if (dir.isDirectory()) {
				for (File notification : dir.listFiles()) {
					try {
						notificator.sendNotification(notification);
						logger.info(notification.getName()
								+ " is successfully sended.");
					} catch (Exception e) {
						// TODO Auto-generated catch block
						logger.error(e.toString());
						logger.error(notification.getName() + " fails to send.");
					}
				}
			}

			notificator.closeConnection();
			notificator.closeChannel();
		} else {
			logger.error("Karma config file not exists.");
		}

		if (ConfigManager.getProperty("notification_delete").equalsIgnoreCase(
				"on")) {
			logger.info("\n" + "Deleting temp notification directory...");
			delete(new File(notification_dir));
		}
	}

	public static void delete(File file) {
		try {
			if (file.isDirectory()) {

				// directory is empty, then delete it
				if (file.list().length == 0) {

					file.delete();
					logger.info("Directory is deleted : "
							+ file.getAbsolutePath());

				} else {

					// list all the directory contents
					String files[] = file.list();

					for (String temp : files) {
						// construct the file structure
						File fileDelete = new File(file, temp);

						// recursive delete
						delete(fileDelete);
					}

					// check the directory again, if empty then delete it
					if (file.list().length == 0) {
						file.delete();
						logger.info("Directory is deleted : "
								+ file.getAbsolutePath());
					}
				}

			} else {
				// if file, then delete it
				file.delete();
				logger.info("File is deleted : " + file.getAbsolutePath());
			}
		} catch (Exception e) {
			logger.error(e.toString());
		}
	}

	public void getWorkflowGraph() {
		System.out.println("\n"
				+ "Karma Client is querying workflow graph...\n");

		String s = null;
		Process p = null;
		String query_xml = "getWorkflowGraphRequest.xml";
		File query = new File(query_xml);

		File query_output = new File(ConfigManager.getProperty("query_path"));
		if (!query_output.exists()) {
			try {
				query_output.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(query_output, false));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		try {
			// System.out.println(KARMA_HOME+ "bin/query.sh " + KARMA_HOME +
			// "config/karma.properties " + query.getAbsolutePath());
			p = Runtime.getRuntime().exec(
					KARMA_HOME + "bin/query.sh " + KARMA_HOME
							+ "config/karma.properties "
							+ query.getAbsolutePath());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		BufferedReader stdInput = new BufferedReader(new InputStreamReader(
				p.getInputStream()));
		try {
			s = stdInput.readLine();
			while (s != null && !s.contains("<v1")) {
				s = stdInput.readLine();
			}

			while (s != null) {
				if (s.contains("<") && !s.contains("</ns")) {
					writer.write(s);
					writer.newLine();
				}
				s = stdInput.readLine();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				writer.flush();
				writer.close();

				stdInput.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		System.out.println("\n"
				+ "Workflow Graph is successfully requested by Karma Client."
				+ "\n");
		p.destroy();
	}
}
