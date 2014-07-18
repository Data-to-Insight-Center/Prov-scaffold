package KarmaAdaptor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.Namespace;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;

import org.apache.log4j.*;

import Util.ConfigManager;
import Util.timeParser;

public class SLOSHAdaptor {

	public Namespace ns = Namespace.getNamespace("ns",
			"http://www.dataandsearch.org/karma/2010/08/");
	public Namespace soapenv = Namespace.getNamespace("soapenv",
			"http://schemas.xmlsoap.org/soap/envelope/");

	private String parsedLogName = null;

	private String notification_dir = ConfigManager
			.getProperty("notification_dir");
	private String bsn_dir = "file://" + SLOSHRun.storage_urls.get("bsn_dir");
	private String trk_dir = "file://" + SLOSHRun.storage_urls.get("trk_dir");
	private String output_dir = "file://"
			+ SLOSHRun.storage_urls.get("output_dir");
	private String merge_dir = "file://"
			+ SLOSHRun.storage_urls.get("merge_dir");

	private String _workflowID;
	private String _workflowNodeID = ConfigManager
			.getProperty("workflowNodeID");
	private String _userDN = ConfigManager.getProperty("userDN");
	private String _usertype = "PERSON";
	private String _email = ConfigManager.getProperty("email");
	private String _workflowinvocationTime = null;
	private String _fileownerDN = "SLOSH";

	private String file_anno = ConfigManager.getProperty("file_annotations");

	private int timestep = 1;
	private Map<String, Element> worker_map = new HashMap<String, Element>();
	// private String timeZone=ConfigManager.getProperty("timeZone");

	static final Logger logger = Logger.getLogger("KarmaAdaptor.SLOSHAdaptor");

	public SLOSHAdaptor(String parsedLog) {
		parsedLogName = parsedLog;
	}

	public String workflowInvoked() {
		logger.info("Extracting workflow invocation provenance information...");

		FileInputStream parsed_Log = null;
		FileInputStream parsed_mid_Log = null;
		try {
			parsed_Log = new FileInputStream(new File(parsedLogName));
			parsed_mid_Log = new FileInputStream(new File(
					"tmp_middleware_log.txt"));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				parsed_Log));
		BufferedReader reader2 = new BufferedReader(new InputStreamReader(
				parsed_mid_Log));

		String line;
		String[] tokens;

		try {
			line = reader.readLine();
			tokens = line.split(" ");
			String time = tokens[5] + " " + tokens[6];
			_workflowinvocationTime = timeParser.dateParser(time);
			_workflowID = "SLOSH-" + _workflowinvocationTime;
			notification_dir = notification_dir + _workflowID + "/";

			logger.info("Workflow ID:" + _workflowID);
			File _notification_dir = new File(notification_dir);
			if (!_notification_dir.exists()) {
				_notification_dir.mkdirs();
			} else {
				String[] _deleteFiles = _notification_dir.list();
				for (int i = 0; i < _deleteFiles.length; i++) {
					File _deleteFile = new File(_notification_dir,
							_deleteFiles[i]);
					_deleteFile.delete();
				}
			}

			line = reader2.readLine();
			tokens = line.split(" ");
			_workflowNodeID = tokens[6];
		} catch (Exception e) {
			logger.error(e.toString());// TODO: handle exception
		} finally {
			try {
				reader.close();
			} catch (IOException ex) {
				// TODO Auto-generated catch block
				logger.error(ex.toString());
			}
		}

		Element userInformation = KarmaElement.userInformation(_userDN,
				_usertype, _email);
		Element workflowInformation = KarmaElement.workflowInformation(
				_workflowID, _workflowNodeID, timestep);
		KarmaNotification.workflowInvoked(userInformation, "USER",
				workflowInformation, "WORKFLOW", _workflowinvocationTime,
				notification_dir + "workflowInvoked.xml");
		logger.info("\n SLOSH WorkflowInvoked Notification Created and Saved!"
				+ "\n");
		timestep++;

		Element masterInformation = KarmaElement.serviceInformation(
				_workflowID, _workflowNodeID, timestep, "Master Node");
		KarmaNotification.serviceInvoked(workflowInformation, "WORKFLOW",
				masterInformation, "SERVICE", _workflowinvocationTime,
				notification_dir + "masterInvoked.xml");

		timestep++;

		FileInputStream middleware_log = null;
		try {
			middleware_log = new FileInputStream(new File(
					"tmp_middleware_log.txt"));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			logger.error(e1.toString());
		}
		BufferedReader middleware_reader = new BufferedReader(
				new InputStreamReader(middleware_log));

		try {
			line = middleware_reader.readLine();
			while (line != null && line.startsWith("worker")) {
				line = line.trim();
				tokens = line.split("\t");
				ArrayList<Element> worker_annotations = SLOSHAnnotations
						.workerAnnotations(tokens[1], tokens[2], tokens[3], timeParser.dateParser(tokens[tokens.length-1]));

				String worker_name = tokens[0];
				String worker_invocationTime = timeParser.dateParser(tokens[4]);

				Element workerInformation = KarmaElement.serviceInformation(
						_workflowID, worker_name, timestep, worker_name,
						worker_annotations);
				KarmaNotification.serviceInvoked(workflowInformation,
						"WORKFLOW", workerInformation, "SERVICE",
						worker_invocationTime, notification_dir + worker_name
								+ "Invoked.xml");

				worker_map.put(worker_name, workerInformation);
				line = middleware_reader.readLine();
				timestep++;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error(e.toString());
		} finally {
			try {
				middleware_reader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				logger.error(e.toString());
			}
		}

		Element submitInformation = KarmaElement.serviceInformation(
				_workflowID, _workflowNodeID, timestep, "Submit Tasks");
		KarmaNotification.serviceInvoked(masterInformation, "SERVICE",
				submitInformation, "SERVICE", _workflowinvocationTime,
				notification_dir + "submitInvoked.xml");
		logger.info("\n SLOSH Submit Tasks ServiceInvoked Notification Created and Saved!"
				+ "\n");
		timestep++;

		return notification_dir;
	}

	public void MiddlewarePhase() {
		logger.info("Extracting SLOSH Distribution phase provenance...");
		Element submitInformation = KarmaElement.serviceInformation(
				_workflowID, _workflowNodeID, timestep - 1, "Submit Tasks");

		FileInputStream parsed_Log = null;
		try {
			parsed_Log = new FileInputStream(new File(parsedLogName));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			logger.error(e1.toString());
		}
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				parsed_Log));

		String line = null;

		int count = 1;
		try {
			line = reader.readLine();
			String[] tokens = line.split(" ");
			System.out.print(tokens[0]);
			String bsn_url = bsn_dir + tokens[0];
			Element bsn_file = KarmaElement.File(bsn_url, _fileownerDN);
			System.out.print(bsn_url);
			Element bsn_object = null;
			if (file_anno.equalsIgnoreCase("on")) {
				ArrayList<Element> bsn_annotations = new SLOSHAnnotations(
						bsn_url).bsnAnnotations();
				bsn_object = KarmaElement.dataObject(bsn_file, bsn_annotations);
			} else {
				bsn_object = KarmaElement.dataObject(bsn_file, null);
			}
			KarmaNotification.dataRelation("PRODUCE", submitInformation,
					"SERVICE", bsn_object, _workflowinvocationTime,
					"File Produced", notification_dir + "bsnProduced.xml");

			while (line != null && !line.startsWith("Merge Information")) {

				tokens = line.split(" ");

				String _serviceNodeID = tokens[4];

				Element currentWorker = worker_map.get(_serviceNodeID);
				
				//Task Success
				if (!tokens[tokens.length - 1].contains("worker")) {
					String trk_url = trk_dir + tokens[1];
					String rex_url = output_dir + tokens[2];
					String env_url = output_dir + tokens[3];

					String consumeTimestamp = timeParser.dateParser(tokens[5]
							+ " " + tokens[6]);
					String produceTimestamp = timeParser.dateParser(tokens[7]
							+ " " + tokens[8]);

					String _simulationInterval = tokens[9];
					String _executionTime = tokens[10];					
					
					String trk_url_short=trk_url.replace("file://","");
					FileDetection detector = new FileDetection(trk_url_short);
               				if(!detector.misplacement())
               				{
						Element failure_trk_file = KarmaElement.File(trk_url, _fileownerDN								, consumeTimestamp, 0, trk_url);
                                        	ArrayList<Element> failure_trk_annotations = null;

                                        	if (file_anno.equalsIgnoreCase("on")) {
                                                	failure_trk_annotations = new SLOSHAnnotations().failureAnnotations("No file exists");				
						
                                        	}

                                        	Element failure_trk_object = KarmaElement.dataObject(failure_trk_file,failure_trk_annotations);
						KarmaNotification.dataRelation("PRODUCE",
                                                        submitInformation, "SERVICE", failure_trk_object,
                                                        _workflowinvocationTime, "File Produced",
                                                        notification_dir + "failuretrkProduced" + count + ".xml");

                                                KarmaNotification.dataRelation("CONSUME", currentWorker,
                                                        "SERVICE", failure_trk_object, consumeTimestamp,
                                                        "File Consumed", notification_dir + "failuretrkConsumed"
                                                                        + count + ".xml");
                                                KarmaNotification.dataRelation("CONSUME", currentWorker,
                                                        "SERVICE", bsn_object, consumeTimestamp,
                                                        "File Consumed", notification_dir + "bsnConsumed"
                                                                        + count + ".xml");

                                                count++;
                                                timestep++;
                                                line = reader.readLine();
					}
					else
					{
						Element trk_file = KarmaElement.File(trk_url, _fileownerDN);
						ArrayList<Element> trk_annotations = null;

						if (file_anno.equalsIgnoreCase("on")) {
							trk_annotations = new SLOSHAnnotations(trk_url)
								.trkAnnotations();
						}

						Element trk_object = KarmaElement.dataObject(trk_file,
							trk_annotations);
		
						Element rex_file = KarmaElement.File(rex_url, _fileownerDN);
						ArrayList<Element> rex_annotations = null;
						if (file_anno.equalsIgnoreCase("on")) {
							rex_annotations = new SLOSHAnnotations()
								.inputrexAnnotations(_simulationInterval,
										_executionTime);
						}

						Element rex_object = KarmaElement.dataObject(rex_file,
							rex_annotations);

						Element env_file = KarmaElement.File(env_url, _fileownerDN);
						ArrayList<Element> env_annotations = null;
						if (file_anno.equalsIgnoreCase("on")) {
							env_annotations = new SLOSHAnnotations()
								.inputenvAnnotations(_simulationInterval,
										_executionTime);
						}
						Element env_object = KarmaElement.dataObject(env_file,
							env_annotations);

						KarmaNotification.dataRelation("PRODUCE",
							submitInformation, "SERVICE", trk_object,
							_workflowinvocationTime, "File Produced",
							notification_dir + "trkProduced" + count + ".xml");


						KarmaNotification.dataRelation("CONSUME", currentWorker,
							"SERVICE", trk_object, consumeTimestamp,
							"File Consumed", notification_dir + "trkConsumed"
									+ count + ".xml");
						KarmaNotification.dataRelation("CONSUME", currentWorker,
							"SERVICE", bsn_object, consumeTimestamp,
							"File Consumed", notification_dir + "bsnConsumed"
									+ count + ".xml");
						KarmaNotification.dataRelation("PRODUCE", currentWorker,
							"SERVICE", env_object, produceTimestamp,
							"File Produced", notification_dir + "envProduced"
									+ count + ".xml");
						KarmaNotification.dataRelation("PRODUCE", currentWorker,
							"SERVICE", rex_object, produceTimestamp,
							"File Produced", notification_dir + "rexProduced"
									+ count + ".xml");

						count++;
						timestep++;
						line = reader.readLine();
					}
				} 
				//Task failure
				else {
					
					String failure_url="return_status:"+tokens[tokens.length-1];
					String trk_url = trk_dir + tokens[1];
					
					String consumeTimestamp = timeParser.dateParser(tokens[5]
							+ " " + tokens[6]);
					String produceTimestamp = timeParser.dateParser(tokens[7]
							+ " " + tokens[8]);
					
FileDetection detector = new FileDetection(trk_url);
                                        if(!detector.misplacement())
                                        {
                                                Element failure_trk_file = KarmaElement.File(trk_url, _fileownerDN                                                              , consumeTimestamp, 0, trk_url);
                                                ArrayList<Element> failure_trk_annotations = null;

                                                if (file_anno.equalsIgnoreCase("on")) {
                                                        failure_trk_annotations = new SLOSHAnnotations().failureAnnotations("No file exists");

                                                }

                                                Element failure_trk_object = KarmaElement.dataObject(failure_trk_file,failure_trk_annotations);
						KarmaNotification.dataRelation("PRODUCE",
                                                        submitInformation, "SERVICE", failure_trk_object,
                                                        _workflowinvocationTime, "File Produced",
                                                        notification_dir + "failuretrkProduced" + count + ".xml");

                                                KarmaNotification.serviceInvoked(currentWorker, "SERVICE",
                                                        currentWorker, "SERVICE", consumeTimestamp,
                                                        notification_dir + "SLOSHInvoked" + count + ".xml");

                                                KarmaNotification.dataRelation("CONSUME", currentWorker,
                                                        "SERVICE", failure_trk_object, consumeTimestamp,
                                                        "File Consumed", notification_dir + "failuretrkConsumed"
                                                                        + count + ".xml");					
					}
					else
					{
						Element trk_file = KarmaElement.File(trk_url, _fileownerDN);
						ArrayList<Element> trk_annotations = null;

						if (file_anno.equalsIgnoreCase("on")) {
							trk_annotations = new SLOSHAnnotations(trk_url)
								.trkAnnotations();
						}

						Element trk_object = KarmaElement.dataObject(trk_file,
							trk_annotations);

						KarmaNotification.dataRelation("PRODUCE",
							submitInformation, "SERVICE", trk_object,
							_workflowinvocationTime, "File Produced",
							notification_dir + "trkProduced" + count + ".xml");

						KarmaNotification.serviceInvoked(currentWorker, "SERVICE",
							currentWorker, "SERVICE", consumeTimestamp,
							notification_dir + "SLOSHInvoked" + count + ".xml");

						KarmaNotification.dataRelation("CONSUME", currentWorker,
							"SERVICE", trk_object, consumeTimestamp,
							"File Consumed", notification_dir + "trkConsumed"
									+ count + ".xml");
					}
					KarmaNotification.dataRelation("CONSUME", currentWorker,
							"SERVICE", bsn_object, consumeTimestamp,
							"File Consumed", notification_dir + "bsnConsumed"
									+ count + ".xml");
					
					Element failure_file = KarmaElement.File(failure_url, _fileownerDN, produceTimestamp, 0, "failure");
					ArrayList<Element> failure_annotations = null;

					if (file_anno.equalsIgnoreCase("on")) {
						failure_annotations = new SLOSHAnnotations().failureAnnotations(tokens[tokens.length-1]);
					}

					Element failure_object = KarmaElement.dataObject(failure_file,
							failure_annotations);
					
					KarmaNotification.dataRelation("PRODUCE", currentWorker,
							"SERVICE", failure_object, produceTimestamp, "File Produced", notification_dir + "failureProduced"
									+ count + ".xml");
					
					count++;
					timestep++;
					line = reader.readLine();
				}
			}

			logger.info("\nSLOSH Distribution Phase Notification Created and Saved!\n");
		} catch (Exception e) {
			logger.error(e.toString());// TODO: handle exception
		} finally {
			try {
				reader.close();
			} catch (IOException ex) {
				// TODO Auto-generated catch block
				logger.error(ex.toString());
			}
		}
	}
	
	public ArrayList<Element> DistributionPhase()
	{
		logger.info("Extracting SLOSH Distribution phase provenance...");
		Element submitInformation = KarmaElement.serviceInformation(
				_workflowID, _workflowNodeID, timestep - 1, "Submit Tasks");

		FileInputStream parsed_Log = null;
		try {
			parsed_Log = new FileInputStream(new File(parsedLogName));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			logger.error(e1.toString());
		}
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				parsed_Log));

		ArrayList<Element> outputenv_list = new ArrayList<Element>();
		String line = null;

		int count = 1;
		try {
			line = reader.readLine();
			String[] tokens = line.split(" ");
			System.out.print(tokens[0]);
			String bsn_url = bsn_dir + tokens[0];
			Element bsn_file = KarmaElement.File(bsn_url, _fileownerDN);
			System.out.print(bsn_url);
			Element bsn_object = null;
			if (file_anno.equalsIgnoreCase("on")) {
				ArrayList<Element> bsn_annotations = new SLOSHAnnotations(
						bsn_url).bsnAnnotations();
				bsn_object = KarmaElement.dataObject(bsn_file, bsn_annotations);
			} else {
				bsn_object = KarmaElement.dataObject(bsn_file, null);
			}
			KarmaNotification.dataRelation("PRODUCE", submitInformation,
					"SERVICE", bsn_object, _workflowinvocationTime,
					"File Produced", notification_dir + "bsnProduced.xml");

			while (line != null && !line.startsWith("Merge Information")) {

				tokens = line.split(" ");

				String _serviceNodeID = tokens[4];

				Element currentWorker = worker_map.get(_serviceNodeID);
				
				//Task Success
				if (!tokens[tokens.length - 1].contains("worker")) {
					String trk_url = trk_dir + tokens[1];
					String rex_url = output_dir + tokens[2];
					String env_url = output_dir + tokens[3];

					String consumeTimestamp = timeParser.dateParser(tokens[5]
							+ " " + tokens[6]);
					String produceTimestamp = timeParser.dateParser(tokens[7]
							+ " " + tokens[8]);

					String _simulationInterval = tokens[9];
					String _executionTime = tokens[10];
					FileDetection detector = new FileDetection(trk_url);
                                        if(!detector.misplacement())
                                        {
                                                Element failure_trk_file = KarmaElement.File(trk_url, _fileownerDN                                                              , consumeTimestamp, 0, trk_url);
                                                ArrayList<Element> failure_trk_annotations = null;

                                                if (file_anno.equalsIgnoreCase("on")) {
                                                        failure_trk_annotations = new SLOSHAnnotations().failureAnnotations("No file exists");

                                                }

                                                Element failure_trk_object = KarmaElement.dataObject(failure_trk_file,failure_trk_annotations);
                                                KarmaNotification.dataRelation("PRODUCE",
                                                        submitInformation, "SERVICE", failure_trk_object,
                                                        _workflowinvocationTime, "File Produced",
                                                        notification_dir + "failuretrkProduced" + count + ".xml");
						
						Element SLOSHInformation = KarmaElement.serviceInformation(
                                                        _workflowID, _serviceNodeID, timestep,
                                                        "SLOSH Application");

						KarmaNotification.serviceInvoked(currentWorker, "SERVICE",
                                                        SLOSHInformation, "SERVICE", consumeTimestamp,
                                                        notification_dir + "SLOSHInvoked" + count + ".xml");

                                        	KarmaNotification.dataRelation("CONSUME", SLOSHInformation,
                                                        "SERVICE", failure_trk_object, consumeTimestamp,
                                                        "File Consumed", notification_dir + "failuretrkConsumed"
                                                                        + count + ".xml");
                                        	KarmaNotification.dataRelation("CONSUME", SLOSHInformation,
                                                        "SERVICE", bsn_object, consumeTimestamp,
                                                        "File Consumed", notification_dir + "bsnConsumed"
                                                                        + count + ".xml");
                                                count++;
                                                timestep++;
                                                line = reader.readLine();
                                        }
                                        else
					{
					Element trk_file = KarmaElement.File(trk_url, _fileownerDN);
					ArrayList<Element> trk_annotations = null;

					if (file_anno.equalsIgnoreCase("on")) {
						trk_annotations = new SLOSHAnnotations(trk_url)
								.trkAnnotations();
					}

					Element trk_object = KarmaElement.dataObject(trk_file,
							trk_annotations);

					Element rex_file = KarmaElement.File(rex_url, _fileownerDN);
					ArrayList<Element> rex_annotations = null;
					if (file_anno.equalsIgnoreCase("on")) {
						rex_annotations = new SLOSHAnnotations()
								.inputrexAnnotations(_simulationInterval,
										_executionTime);
					}

					Element rex_object = KarmaElement.dataObject(rex_file,
							rex_annotations);

					Element env_file = KarmaElement.File(env_url, _fileownerDN);
					ArrayList<Element> env_annotations = null;
					if (file_anno.equalsIgnoreCase("on")) {
						env_annotations = new SLOSHAnnotations()
								.inputenvAnnotations(_simulationInterval,
										_executionTime);
					}
					Element env_object = KarmaElement.dataObject(env_file,
							env_annotations);

					Element SLOSHInformation = KarmaElement.serviceInformation(
							_workflowID, _serviceNodeID, timestep,
							"SLOSH Application");

					KarmaNotification.dataRelation("PRODUCE",
							submitInformation, "SERVICE", trk_object,
							_workflowinvocationTime, "File Produced",
							notification_dir + "trkProduced" + count + ".xml");

					KarmaNotification.serviceInvoked(currentWorker, "SERVICE",
							SLOSHInformation, "SERVICE", consumeTimestamp,
							notification_dir + "SLOSHInvoked" + count + ".xml");

					KarmaNotification.dataRelation("CONSUME", SLOSHInformation,
							"SERVICE", trk_object, consumeTimestamp,
							"File Consumed", notification_dir + "trkConsumed"
									+ count + ".xml");
					KarmaNotification.dataRelation("CONSUME", SLOSHInformation,
							"SERVICE", bsn_object, consumeTimestamp,
							"File Consumed", notification_dir + "bsnConsumed"
									+ count + ".xml");
					KarmaNotification.dataRelation("PRODUCE", SLOSHInformation,
							"SERVICE", env_object, produceTimestamp,
							"File Produced", notification_dir + "envProduced"
									+ count + ".xml");
					KarmaNotification.dataRelation("PRODUCE", SLOSHInformation,
							"SERVICE", rex_object, produceTimestamp,
							"File Produced", notification_dir + "rexProduced"
									+ count + ".xml");

					outputenv_list.add(env_object);

					count++;
					timestep++;
					line = reader.readLine();
				} 
				}	
				//Task failure
				else {
					
					String failure_url="return_status:"+tokens[tokens.length-1];
					String trk_url = trk_dir + tokens[1];
					
					String consumeTimestamp = timeParser.dateParser(tokens[5]
							+ " " + tokens[6]);
					String produceTimestamp = timeParser.dateParser(tokens[7]
							+ " " + tokens[8]);

					Element trk_file = KarmaElement.File(trk_url, _fileownerDN);
					ArrayList<Element> trk_annotations = null;

					if (file_anno.equalsIgnoreCase("on")) {
						trk_annotations = new SLOSHAnnotations(trk_url)
								.trkAnnotations();
					}
				 	
					FileDetection detector = new FileDetection(trk_url);
                                        if(!detector.misplacement())
                                        {
						Element failure_trk_file = KarmaElement.File(trk_url, _fileownerDN                                                              ,consumeTimestamp, 0, trk_url);
                                                ArrayList<Element> failure_trk_annotations = null;

                                                if (file_anno.equalsIgnoreCase("on")) {
                                                        failure_trk_annotations = new SLOSHAnnotations().failureAnnotations("No file exists");

                                                }

                                                Element failure_trk_object = KarmaElement.dataObject(failure_trk_file,failure_trk_annotations);

						Element SLOSHInformation = KarmaElement.serviceInformation(
                                                        _workflowID, _serviceNodeID, timestep,
                                                        "SLOSH Application");

                                        	KarmaNotification.dataRelation("PRODUCE",
                                                        submitInformation, "SERVICE", failure_trk_object,
                                                        _workflowinvocationTime, "File Produced",
                                                        notification_dir + "failuretrkProduced" + count + ".xml");

                                        	KarmaNotification.serviceInvoked(currentWorker, "SERVICE",
                                                        SLOSHInformation, "SERVICE", consumeTimestamp,
                                                        notification_dir + "SLOSHInvoked" + count + ".xml");

                                        	KarmaNotification.dataRelation("CONSUME", SLOSHInformation,
                                                        "SERVICE", failure_trk_object, consumeTimestamp,
                                                        "File Consumed", notification_dir + "failuretrkConsumed"
                                                                        + count + ".xml");
                                        	KarmaNotification.dataRelation("CONSUME", SLOSHInformation,
                                                        "SERVICE", bsn_object, consumeTimestamp,
                                                        "File Consumed", notification_dir + "bsnConsumed"
                                                                        + count + ".xml");

                                        	Element failure_file = KarmaElement.File(failure_url, _fileownerDN,produceTimestamp, 0, "failure");
						 ArrayList<Element> failure_annotations = null;

                                        	if (file_anno.equalsIgnoreCase("on")) {
                                                	failure_annotations = new SLOSHAnnotations().failureAnnotations(tokens[tokens.length-1]);
                                        	}

                                        	Element failure_object = KarmaElement.dataObject(failure_file,
                                                        failure_annotations);

                                        	KarmaNotification.dataRelation("PRODUCE", SLOSHInformation,
                                                        "SERVICE", failure_object, produceTimestamp, "File Produced", notification_dir + "failureProduced"
                                                                        + count + ".xml");

                                        	count++;
                                        	timestep++;
                                        	line = reader.readLine();	
					}
					else
					{
					Element trk_object = KarmaElement.dataObject(trk_file,
							trk_annotations);
	
					Element SLOSHInformation = KarmaElement.serviceInformation(
							_workflowID, _serviceNodeID, timestep,
							"SLOSH Application");

					KarmaNotification.dataRelation("PRODUCE",
							submitInformation, "SERVICE", trk_object,
							_workflowinvocationTime, "File Produced",
							notification_dir + "trkProduced" + count + ".xml");

					KarmaNotification.serviceInvoked(currentWorker, "SERVICE",
							SLOSHInformation, "SERVICE", consumeTimestamp,
							notification_dir + "SLOSHInvoked" + count + ".xml");

					KarmaNotification.dataRelation("CONSUME", SLOSHInformation,
							"SERVICE", trk_object, consumeTimestamp,
							"File Consumed", notification_dir + "trkConsumed"
									+ count + ".xml");
					KarmaNotification.dataRelation("CONSUME", SLOSHInformation,
							"SERVICE", bsn_object, consumeTimestamp,
							"File Consumed", notification_dir + "bsnConsumed"
									+ count + ".xml");
					
					Element failure_file = KarmaElement.File(failure_url, _fileownerDN,produceTimestamp, 0, "failure");
					ArrayList<Element> failure_annotations = null;

					if (file_anno.equalsIgnoreCase("on")) {
						failure_annotations = new SLOSHAnnotations().failureAnnotations(tokens[tokens.length-1]);
					}

					Element failure_object = KarmaElement.dataObject(failure_file,
							failure_annotations);
					
					KarmaNotification.dataRelation("PRODUCE", SLOSHInformation,
							"SERVICE", failure_object, produceTimestamp, "File Produced", notification_dir + "failureProduced"
									+ count + ".xml");
					
					count++;
					timestep++;
					line = reader.readLine();
					}
				}
			}

			logger.info("\nSLOSH Distribution Phase Notification Created and Saved!\n");
		} catch (Exception e) {
			logger.error(e.toString());// TODO: handle exception
		} finally {
			try {
				reader.close();
			} catch (IOException ex) {
				// TODO Auto-generated catch block
				logger.error(ex.toString());
			}
		}

		return outputenv_list;
	}
	
	public void MergePhase(ArrayList<Element> outputenv_list) {
		logger.info("Extracting SLOSH Merge Phase provenance...");
		FileInputStream parsed_Log = null;
		try {
			parsed_Log = new FileInputStream(new File(parsedLogName));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			logger.error(e.toString());
		}
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				parsed_Log));

		Element mergeInformation = KarmaElement.serviceInformation(_workflowID,
				_workflowNodeID, timestep, "Merge/Aggregation");

		String line = null;
		String[] tokens = null;

		ArrayList<Element> mergeenv_list = new ArrayList<Element>();
		String consumeTimestamp = null;
		String produceTimestamp = null;

		try {
			line = reader.readLine();
			while (!line.startsWith("Merge Information") && line != null) {
				line = reader.readLine();
			}
			line = reader.readLine();
			while (line != null && !line.contains("execution")) {
				line = line.trim();
				if (line.startsWith("start:")) {
					tokens = line.split(":");
					Long consumeTime = Long.parseLong(tokens[1].trim());
					consumeTimestamp = timeParser.epochParser(consumeTime);
				} else if (line.startsWith("end:")) {
					tokens = line.split(":");
					Long produceTime = Long.parseLong(tokens[1].trim());
					produceTimestamp = timeParser.epochParser(produceTime);
				} else {
					tokens = line.split(";");
					String mergeenv_url = merge_dir + tokens[0].trim();
					String stormCAT = tokens[1].trim();
					String stormDirection = tokens[2].trim();
					String speed = tokens[3].trim();
					String datums = tokens[4].trim();

					Element mergeenv_file = KarmaElement.File(mergeenv_url,
							_fileownerDN);
					ArrayList<Element> mergeenv_annotations = null;
					if (file_anno.equalsIgnoreCase("on")) {
						mergeenv_annotations = new SLOSHAnnotations()
								.outputenvAnnotations(stormCAT, stormDirection,
										speed, datums);
					}
					Element mergeenv_object = KarmaElement.dataObject(
							mergeenv_file, mergeenv_annotations);
					mergeenv_list.add(mergeenv_object);
				}
				line = reader.readLine();
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error(e.toString());
		}

		int count = 1;
		for (Element outputenv : outputenv_list) {
			KarmaNotification.dataRelation("CONSUME", mergeInformation,
					"SERVICE", outputenv, consumeTimestamp, "File Consumed",
					notification_dir + "merge_envConsumed" + count + ".xml");
			count++;
		}

		count = 1;
		for (Element mergeenv : mergeenv_list) {
			KarmaNotification.dataRelation("PRODUCE", mergeInformation,
					"SERVICE", mergeenv, produceTimestamp, "File Produced",
					notification_dir + "merge_envProduced" + count + ".xml");
			count++;
		}

		logger.info("\nSLOSH Merge/Aggregate Phase Notification Created and Saved!\n");
	}
}
