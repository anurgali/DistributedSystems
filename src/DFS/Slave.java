package DFS;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import MR.IMapper;
import MR.IReducer;
import MR.MRFactory;
import MR.WordCountMapper;
import MR.WordCountReducer;

import util.Config;
import util.Converter;


public class Slave {

	private int port, masterport;
	private String ip, masterIp;
	private DatagramSocket slaveSocket;
	private Logger logger = Logger.getLogger("Slave Log");
	private int length=Integer.parseInt(Config.getString("length"));
	
	//client->folder->files
	private HashMap<IpPort, TreeMap<String, ArrayList<File>>> directory=
			new HashMap<IpPort, TreeMap<String, ArrayList<File>>>();
	private final byte INITIALIZE=1, 
			FILE_READ=2, 
			FILE_WRITE=3, 
			FILE_DELETE=4, 
			FILE_INFO=5, 
			OPEN_DIR=6, 
			READ_DIR=7, 
			MAKE_DIR=8, 
			DELETE_DIR=9,
			MAP=101,
			REDUCE=102;
	
	public Slave(String ip, String port) throws SocketException, UnknownHostException{
		this.port=Integer.parseInt(port);
		this.ip=ip;	
		InetAddress IPAddress = InetAddress.getByName(ip);
		slaveSocket = new DatagramSocket(this.port, IPAddress);
		try {  
	    	FileHandler fh = new FileHandler("slave_"+ip+"_"+port);  
	        logger.addHandler(fh);
	        SimpleFormatter formatter = new SimpleFormatter();  
	        fh.setFormatter(formatter);  
	    } catch (SecurityException e) {  
	        e.printStackTrace();  
	    } catch (IOException e) {  
	        e.printStackTrace();  
	    }  
	}
	
	public void run() throws IOException{
		byte[] receiveData = new byte[length];
		try{
			while (true) {
				DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
				slaveSocket.receive(receivePacket);
				receiveData = receivePacket.getData();
				byte[] sendData=parseRequest(receiveData, receivePacket);
				if (sendData[0]!=0){
					DatagramPacket sendPacket = new DatagramPacket(sendData,
							sendData.length, receivePacket.getAddress(), receivePacket.getPort());
					slaveSocket.send(sendPacket);
				}
			}
		} finally{
			slaveSocket.close();
		}
	}

	private byte[] parseRequest(byte[] receiveData, DatagramPacket receivePacket) throws IOException {
		byte[] sendData=new byte[length];
		
		byte header=receiveData[0];
		byte[] addrInBytes = Arrays.copyOfRange(receiveData, 1, 22);
		byte[] pathInBytes=Arrays.copyOfRange(receiveData, 22, 43);
		byte[] msgInBytes = Arrays.copyOfRange(receiveData, 43, length);

		String addr = new String(addrInBytes);
		addr=addr.trim();
		if (addr.length()==0){
			return sendData;
		}
		
		String msg=new String(msgInBytes);
		msg=msg.trim();
		
		String[] split = addr.split(":");
		addr=addr.replaceFirst(":", "_");
		InetAddress clientAddress = InetAddress.getByName(split[0]);
		int clientPort = Integer.parseInt(split[1]);
		IpPort _clientAddress = new IpPort(clientAddress, clientPort);
		String path=new String(pathInBytes);
		path=addr+"\\"+path.trim();
		File f = null;
		TreeMap<String, ArrayList<File>> folders = directory.get(_clientAddress);
		switch (header){
		case INITIALIZE:
			f=new File(addr);
			if (f.isDirectory() && f.exists()){
				deleteDirectory(f);
			}
			if (f.mkdir()){
				directory.put(_clientAddress, new TreeMap<String, ArrayList<File>>());
			}
			sendData[0]=INITIALIZE+10;
			break;
		case FILE_READ:
			f=new File(path);
			if (f.isFile()){
				ArrayList<File> list = folders.get(f.getParent());
				int index=list.indexOf(f);
				if (index==-1){
					sendData[0]=FILE_READ*-1;					
				}
				else{
					f=list.get(index);
					FileReader fr=new FileReader(f);
					char[] cbuf=new char[1000];
					fr.read(cbuf, 0, cbuf.length);
					String s=new String(cbuf);
					byte[] res=s.getBytes();
					sendData[0]=FILE_READ+10;
					fr.close();
					sendData=merge(sendData, res);
				}
			}
			else{
				sendData[0]=FILE_READ*-1;
			}
			break;
		case FILE_WRITE:
			f=new File(path);
			if (f.createNewFile()){
				PrintWriter pwPrintWriter = new PrintWriter(f);
				pwPrintWriter.println(msg);
				pwPrintWriter.flush();
				pwPrintWriter.close();
				directory.get(_clientAddress).get(f.getParent()).add(f);
				sendData[0]=FILE_WRITE+10;
			}
			else{
				sendData[0]=FILE_WRITE*-1;
			}
			break;
		case FILE_DELETE:
			f=new File(path);
			if (f.exists() && f.isFile() && f.delete()){
				folders.get(f.getParent()).remove(f);
				sendData[0]=FILE_DELETE+10;
			}
			else{
				sendData[0]=FILE_DELETE*-1;
			}
			break;
		case FILE_INFO:
			f=new File(path);
			StringBuilder result=new StringBuilder();
			result.append("Hidden: "+f.isHidden()+"\t")
			.append("Last modified: "+new Date(f.lastModified())+"\t")
			.append("Length: "+f.length() +" bytes");
			sendData[0]=FILE_INFO+10;
			byte[] r= result.toString().getBytes();
			sendData=merge(sendData, r);
			break;
		case OPEN_DIR:
			break;
		case READ_DIR:
			f=new File(path);
			String[] filesList=f.list();
//			ArrayList<File> files = folders.get(path);
			result=new StringBuilder();
//			for (File _f: files){
			for (String s: filesList){
				result.append(s+"\t");
			}
			sendData[0]=READ_DIR+10;
			r= result.toString().getBytes();
			sendData=merge(sendData, r);
			break;
		case MAKE_DIR:		
			f = new File(path);
			if (f.mkdirs() && folders!=null && path!=null){
				folders.put(path, new ArrayList<File>());
			}
			sendData[0]=MAKE_DIR+10;
			break;
		case DELETE_DIR:
			f=new File(path);
			if (f.isDirectory() && deleteDirectory(f)){
				folders.remove(path);
				sendData[0]=DELETE_DIR+10;
			}
			else{
				sendData[0]=DELETE_DIR*-1;
			}
			break;		
		case MAP:
			logger.info("Received MAP");
			IMapper mapper = MRFactory.getMapper(Config.getString("example"));
			Map<String, List<Object>> output=new TreeMap<String, List<Object>>();
			mapper.map(path, msg, output);
			byte[] outputInBytes=Converter.convertToBytes(output);
			sendData=new byte[outputInBytes.length+1]; 
			sendData[0]=MAP+10;
			sendData=merge(sendData, outputInBytes);
			break;
		case REDUCE:
			logger.info("Received REDUCE");
			IReducer reducer=MRFactory.getReducer(Config.getString("example"));
			try {
				Map<String, List<Object>> input=(Map<String, List<Object>>)Converter.createObject(msgInBytes);
				String reduceOutput = reducer.reduce(input);
				f=new File(path);
				if (!f.exists()){
					boolean success=f.createNewFile();
					if (!success){
						sendData[0]=REDUCE*-1;
						break;
					}
				}
				FileWriter fw = new FileWriter(path, true);
				fw.append(reduceOutput);
				fw.close();
				sendData[0]=REDUCE+10;
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}			
			break;
		default:
			break;
		}
		return sendData;
	}

	private byte[] merge(byte[] result, byte[] bytes) {
		for (int i=0; i<result.length; i++){
			if (result[i]==0){
				for (int j=0; j<bytes.length; j++){
					result[j+i]=bytes[j];
				}
				break;
			}
		}
		return result;
	}
	
	private boolean deleteDirectory(File path) {
	    if( path.exists() ) {
	      File[] files = path.listFiles();
	      for(int i=0; i<files.length; i++) {
	         if(files[i].isDirectory()) {
	           deleteDirectory(files[i]);
	         }
	         else {
	           files[i].delete();
	         }
	      }
	    }
	    return( path.delete() );
	  }
}
