package KarmaAdaptor;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

import java.util.ArrayList;
import java.util.Date;
import org.jdom2.Element;
import org.jdom2.Namespace;
import Util.timeParser;

public class KarmaElement {
	public static Namespace ns = Namespace.getNamespace("ns","http://www.dataandsearch.org/karma/2010/08/");
	public static Namespace soapenv = Namespace.getNamespace("soapenv","http://schemas.xmlsoap.org/soap/envelope/");

	
	public static Element userInformation(String _userDN, String _type, String _email)
	{
		Element userInformation = new Element("userInformation", ns);
		userInformation.addContent(new Element("userDN", ns).setText(_userDN));
		userInformation.addContent(new Element("type", ns).setText(_type));
		userInformation.addContent(new Element("email", ns).setText(_email));
		
		return userInformation;
	}
	
	public static Element workflowInformation(String _workflowID, String _workflowNodeID, int _timestep)
	{
		Element workflowInformation = new Element("workflowInformation", ns);
		workflowInformation.addContent(new Element("workflowID", ns).setText(_workflowID));
		workflowInformation.addContent(new Element("workflowNodeID", ns).setText(_workflowNodeID));
		workflowInformation.addContent(new Element("timestep", ns).setText(Integer.toString(_timestep)));
		
		return workflowInformation;
	}
	
	public static Element serviceInformation(String _workflowID, String _workflowNodeID, 
			
			int _timestep, String _serviceID)
	{
		Element serviceInformation=new Element("serviceInformation",ns);
		serviceInformation.addContent(new Element("workflowID",ns).setText(_workflowID));
		serviceInformation.addContent(new Element("workflowNodeID",ns).setText(_workflowNodeID));
		serviceInformation.addContent(new Element("timestep",ns).setText(Integer.toString(_timestep)));
		serviceInformation.addContent(new Element("serviceID",ns).setText(_serviceID));
		
		return serviceInformation;
	}
	
	public static Element serviceInformation(String _workflowID, String _workflowNodeID, 
			int _timestep, String _serviceID, ArrayList<Element> annotations)
	{
		Element serviceInformation=new Element("serviceInformation",ns);
		serviceInformation.addContent(new Element("workflowID",ns).setText(_workflowID));
		serviceInformation.addContent(new Element("workflowNodeID",ns).setText(_workflowNodeID));
		serviceInformation.addContent(new Element("timestep",ns).setText(Integer.toString(_timestep)));
		for(Element annotation: annotations)	
			serviceInformation.addContent(annotation);
		serviceInformation.addContent(new Element("serviceID",ns).setText(_serviceID));
		
		return serviceInformation;
	}
	
	public static Element File(String file_url, String _ownerDN)
	{
		URL url=null;
		try {
			url = new URL( file_url);
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		File inputFile=new File(url.getFile());
		
		URLConnection urlConnection=null;
		try {
			urlConnection = url.openConnection();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		String _createDate=timeParser.dateParser(new Date(urlConnection.getLastModified()));
		int _size=urlConnection.getContentLength();
		String _objectName =inputFile.getName();
		
		Element file =new Element("file",ns);
		file.addContent(new Element("fileURI",ns).setText(file_url));
		file.addContent(new Element("ownerDN",ns).setText(_ownerDN));
		file.addContent(new Element("createDate",ns).setText(_createDate));
		file.addContent(new Element("size",ns).setText(Integer.toString(_size)));
		file.addContent(new Element("objectName",ns).setText(_objectName));
		
		return file;
	}
		
	public static Element File(String file_url, String _ownerDN, String _createDate, int _size, String _objectName)
        {
                Element file =new Element("file",ns);
                file.addContent(new Element("fileURI",ns).setText(file_url));
                file.addContent(new Element("ownerDN",ns).setText(_ownerDN));
                file.addContent(new Element("createDate",ns).setText(_createDate));
                file.addContent(new Element("size",ns).setText(Integer.toString(_size)));
                file.addContent(new Element("objectName",ns).setText(_objectName));

                return file;
        }

	public static Element annotations(String _property, String _value)
	{
		Element annotations =new Element("annotations",ns);
		
		annotations.addContent(new Element("property",ns).setText(_property));
		Element value=new Element("value",ns);
		annotations.addContent(value.setText(_value));
		
		return annotations;
	}
	
	public static Element annotations(String _property, ArrayList<String> _value)
	{
		Element annotations =new Element("annotations",ns);
		
		annotations.addContent(new Element("property",ns).setText(_property));
		Element value=new Element("value",ns);
		
		for(String _onevalue: _value)
			value.addContent(new Element("value").setText(_onevalue));
		
		annotations.addContent(value);
		
		return annotations;
	}
	
	public static Element dataObject(Element file, ArrayList<Element> annotations)
	{
		Element dataObject=new Element("dataObject",ns);
		dataObject.addContent(file);
		if(annotations!=null)
		{
			for(Element annotation: annotations)	
				dataObject.addContent(annotation);
		}
		
		return dataObject;
	}
	
}
