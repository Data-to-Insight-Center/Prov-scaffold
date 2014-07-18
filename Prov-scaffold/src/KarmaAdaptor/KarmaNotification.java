package KarmaAdaptor;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

import java.util.ArrayList;
import java.util.Date;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.Namespace;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;

import Util.timeParser;

public class KarmaNotification {
	
	public static Namespace ns = Namespace.getNamespace("ns","http://www.dataandsearch.org/karma/2010/08/");
	public static Namespace soapenv = Namespace.getNamespace("soapenv","http://schemas.xmlsoap.org/soap/envelope/");
	
	public static void workflowInvoked(Element _invoker, String _invokerType, 
Element _invokee, String _invokeeType, String _invocationTime, String output_path)
	{
		Element _newinvoker=_invoker.clone();
		Element _newinvokee=_invokee.clone();
		
		try {
			Document doc=new Document();
			Element root = new Element("workflowInvoked", ns);
			doc.setRootElement(root);

			Element invoker = new Element("invoker", ns);
			invoker.addContent(_newinvoker);
			invoker.addContent(new Element("type", ns).setText(_invokerType));

			doc.getRootElement().addContent(invoker);

			Element invokee = new Element("invokee", ns);
			invokee.addContent(_newinvokee);
			invokee.addContent(new Element("type", ns).setText(_invokeeType));

			doc.getRootElement().addContent(invokee);

			doc.getRootElement().addContent(
					new Element("invocationTime", ns).setText(_invocationTime));

			// new XMLOutputter().output(doc, System.out);
			XMLOutputter xmlOutput = new XMLOutputter();

			// display nice nice
			xmlOutput.setFormat(Format.getPrettyFormat());
			xmlOutput.output(doc, new FileWriter(output_path));

		} catch (IOException io) {
			System.out.println(io.getMessage());
		}
	}

	public static void serviceInvoked(Element _invoker, String _invokerType, 
Element _invokee, String _invokeeType, String _invocationTime, String output_path)
	{
		Element _newinvoker=_invoker.clone();
		Element _newinvokee=_invokee.clone();
		try {
			Document doc=new Document();
			Element root = new Element("serviceInvoked", ns);
			doc.setRootElement(root);

			Element invoker = new Element("invoker", ns);
			invoker.addContent(_newinvoker);
			invoker.addContent(new Element("type", ns).setText(_invokerType));

			doc.getRootElement().addContent(invoker);

			Element invokee = new Element("invokee", ns);
			invokee.addContent(_newinvokee);
			invokee.addContent(new Element("type", ns).setText(_invokeeType));

			doc.getRootElement().addContent(invokee);

			doc.getRootElement().addContent(
					new Element("invocationTime", ns).setText(_invocationTime));

			// new XMLOutputter().output(doc, System.out);
			XMLOutputter xmlOutput = new XMLOutputter();

			// display nice nice
			xmlOutput.setFormat(Format.getPrettyFormat());
			xmlOutput.output(doc, new FileWriter(output_path));

		} catch (IOException io) {
			System.out.println(io.getMessage());
		}
	}
	
	public static void dataRelation(String _action, Element _actor, String _actorType, Element _dataObject, String _timestamp, String _dataRole, String output_path)
	{
		Element _newactor=_actor.clone();
		Element _newdataObject=_dataObject.clone();
		try {
			
			Document doc=new Document();
			Element root = null;
			
			if (_action.equalsIgnoreCase("CONSUME")) {
				root = new Element("dataConsumed", ns);
			} else {
				root = new Element("dataProduced", ns);
			}
			doc.setRootElement(root);

			doc.getRootElement().addContent(
					new Element("action", ns).setText(_action));

			Element actor = new Element("actor", ns);
			actor.addContent(_newactor);
			actor.addContent(new Element("type", ns).setText(_actorType));

			doc.getRootElement().addContent(actor);

			doc.getRootElement().addContent(_newdataObject);

			doc.getRootElement().addContent(
					new Element("timestamp", ns).setText(_timestamp));

			doc.getRootElement().addContent(
					new Element("dataRole", ns).setText(_dataRole));

			// new XMLOutputter().output(doc, System.out);
			XMLOutputter xmlOutput = new XMLOutputter();

			// display nice nice
			xmlOutput.setFormat(Format.getPrettyFormat());

			xmlOutput.output(doc, new FileWriter(output_path));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
