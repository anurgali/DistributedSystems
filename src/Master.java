import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * 
 * @author anurgali
 *
 */
public class Master {

	private File slaves;
	private int port;
	private Logger logger = Logger.getLogger("Server Log");  
	private String ip;
	private ArrayList<IpPort> _slaves=new ArrayList<IpPort>();
	private HashMap<IpPort, String> currentPaths=new HashMap<IpPort, String>();
	private HashMap<IpPort,TreeMap<String,IpPort>> metadata=new HashMap<IpPort, TreeMap<String,IpPort>>();
	private DatagramSocket serverSocket;
	private final byte INITIALIZE=1, 
			FILE_READ=2, 
			FILE_WRITE=3, 
			FILE_DELETE=4, 
			FILE_INFO=5, 
			OPEN_DIR=6, 
			READ_DIR=7, 
			MAKE_DIR=8, 
			DELETE_DIR=9;
	
	public Master(String ip, String port, String slavesfile, String logFile) 
			throws FileNotFoundException, ParseException, UnknownHostException {
		slaves = new File(slavesfile);
		if (!slaves.exists()) {
			throw new FileNotFoundException(
					"The slaves file is not found! Try again");
		}
		try {  
	    	FileHandler fh = new FileHandler(logFile);  
	        logger.addHandler(fh);
	        SimpleFormatter formatter = new SimpleFormatter();  
	        fh.setFormatter(formatter);  
	    } catch (SecurityException e) {  
	        e.printStackTrace();  
	    } catch (IOException e) {  
	        e.printStackTrace();  
	    }  
		this.port=Integer.parseInt(port);
		this.ip=ip;		
		parseSlaves();
	}
	
	private void parseSlaves() throws FileNotFoundException, NumberFormatException, UnknownHostException{
		Scanner sc=new Scanner(slaves);
		while(sc.hasNextLine()){
			String ipPort=sc.nextLine();
			String[] split=ipPort.split(":");
			String ip=split[0];
			String port=split[1];
			int _port=Integer.parseInt(port);
			if (this.ip.equals(ip) && this.port==_port){
				continue;
			}
			IpPort tuple = new IpPort(InetAddress.getByName(ip), Integer.parseInt(port));
			_slaves.add(tuple);
		}
		sc.close();
	}

	public void run() throws IOException {
		InetAddress IPAddress = InetAddress.getByName(ip);
		serverSocket = new DatagramSocket(port, IPAddress);
		byte[] receiveData = new byte[1024];
		byte[] sendData = new byte[1024];
		try{
			while (true) {
				DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
				serverSocket.setSoTimeout(0);
				serverSocket.receive(receivePacket);
				receiveData = receivePacket.getData();
				sendData=parseRequest(receiveData, receivePacket);
				if (sendData[0]!=0){
					DatagramPacket sendPacket = new DatagramPacket(sendData,
							sendData.length, receivePacket.getAddress(), receivePacket.getPort());
					serverSocket.send(sendPacket);
				}
			}
		} finally{
			serverSocket.close();
		}
	}

	
	//INITIALIZE=0, FILE_READ=1, FILE_WRITE=2, FILE_DELETE=3, FILE_INFO=4,
	//OPEN_DIR=5, READ_DIR=6, MAKE_DIR=7, DELETE_DIR=8;
	private byte[] parseRequest(byte[] receiveData, DatagramPacket receivePacket) throws IOException {
		byte[] copy = Arrays.copyOfRange(receiveData, 1, 22);
		byte[] copy2 = Arrays.copyOfRange(receiveData, 22, receiveData.length);
		String path=new String(copy);
		String msg=new String(copy2);
		path=path.trim();
		msg=msg.trim();
		InetAddress clientAddress = receivePacket.getAddress();
		int clientPort = receivePacket.getPort();		
		IpPort _clientAddress=new IpPort(clientAddress, clientPort),
				slave=null;
		byte command=receiveData[0];
		byte[] result=new byte[1024];
		TreeMap<String, IpPort> slaveTree = metadata.get(_clientAddress);
		
		File file = new File(path);
		String parent=file.getParent(),
				fullPath="";
		if (parent==null){
			parent=currentPaths.get(_clientAddress);
			if (parent!=null && !parent.equals("\\") && !path.equals(".."))
				fullPath=parent+"\\"+path;
			else if (path.equals("..")){
				File ff=new File(parent);
				fullPath=ff.getParent();
				parent=fullPath;
			}
			else
				fullPath=path;
		}
		else{
			fullPath=path;
		}
		slave = findSlave(parent, _clientAddress);
		
		switch (command){
		case INITIALIZE:
			for (IpPort addr: _slaves){
				byte[] sendData=new byte[1024];
				sendData[0]=INITIALIZE;
				byte[] addrInBytes=_clientAddress.toString().getBytes();
				for (int i=0; i< addrInBytes.length; i++){
					sendData[i+1]=addrInBytes[i];
				}
				DatagramPacket sendPacket = new DatagramPacket(sendData,
						sendData.length, addr.ip, addr.port);
				serverSocket.send(sendPacket);
				result=waitForSlaveReply(command);
				if (result[0]==(INITIALIZE*-1)){
					break;
				}
			}
			if (result[0]!=(INITIALIZE*-1)){
				metadata.put(_clientAddress, new TreeMap<String, IpPort>());
				currentPaths.put(_clientAddress, "\\");
			}
//			result[0]=INITIALIZE;
			break;
		case FILE_READ:
		case FILE_WRITE:
		case FILE_DELETE:
		case FILE_INFO:
			if (slave==null){
				result[0] = (byte) (command*-1);
			}
			else{
				sendCommandToSlave(slave, fullPath, _clientAddress, command, msg);
				result=waitForSlaveReply(command);
			}
			break;
		case OPEN_DIR:
			if (metadata.get(_clientAddress).containsKey(fullPath)){
				currentPaths.put(_clientAddress, fullPath);
				result[0]=OPEN_DIR+10;
			}
			else{
				result[0]=OPEN_DIR*-1;
			}
			break;
		case READ_DIR:
			fullPath=currentPaths.get(_clientAddress);
			slave=slaveTree.get(fullPath);
			sendCommandToSlave(slave, fullPath, _clientAddress, READ_DIR, null);
			result=waitForSlaveReply(command);
			break;
		case MAKE_DIR:
			int index=fullPath.hashCode() % _slaves.size();
			slave = _slaves.get(index);//this is the slave where the folder should be created.
			sendCommandToSlave(slave, fullPath, _clientAddress, MAKE_DIR, null);
			result=waitForSlaveReply(command);
			slaveTree.put(fullPath, slave);
//			result[0]=MAKE_DIR;
			break;
		case DELETE_DIR:
			if (slaveTree.containsKey(fullPath)){
				slave = slaveTree.get(fullPath);
				sendCommandToSlave(slave, fullPath, _clientAddress, DELETE_DIR, null);
				slaveTree.remove(fullPath);
				result[0]=DELETE_DIR;
			}
			else{
				result[0]=-1*DELETE_DIR;
			}
			break;
		default:
			System.out.println("Unknown command "+command);
			break;
		}
		return result;
	}

	private void sendCommandToSlave(IpPort s, String path, IpPort clientAddress, byte command, String msg) throws IOException {
		byte[] sendData = new byte[1024];
		byte[] addrInBytes=clientAddress.toString().getBytes();
		byte[] pathInBytes = path.getBytes();
		byte[] msgInBytes = {};
		if (msg!=null)
			msgInBytes=msg.getBytes();
		sendData[0]=command;
		for (int i=0; i<addrInBytes.length; i++){
			sendData[i+1]=addrInBytes[i];
		}
		for (int i=0; i<pathInBytes.length; i++){
			sendData[i+22]=pathInBytes[i];
		}
		for (int i=0; i<msgInBytes.length; i++){
			sendData[i+43]=msgInBytes[i];
		}
		DatagramPacket sendPacket = new DatagramPacket(sendData,
				sendData.length, s.ip, s.port);
		serverSocket.send(sendPacket);
	}
	
	private byte[] waitForSlaveReply(byte command) throws IOException{
		byte[] receiveData=new byte[1024];
		try{
			DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
			serverSocket.setSoTimeout(3000);
			serverSocket.receive(receivePacket);
			receiveData = receivePacket.getData();
		}catch(SocketTimeoutException t){
			receiveData[0]=(byte) (command*-1);			
		}
		return receiveData;
	}


	private IpPort findSlave(String path, IpPort clientAddress) {
		if (path==null)
			return null;
		TreeMap<String, IpPort> paths = metadata.get(clientAddress);
		return paths.get(path);
	}



}
