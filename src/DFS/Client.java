package DFS;
import java.io.*;
import java.net.*;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Scanner;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import util.Config;

/**
 * 
 * @author anurgali
 *
 */
public class Client {

	private int port, masterPort;
	private int length=Integer.parseInt(Config.getString("length"));
	private String ip, masterIp;
	private String cmdStr="cmd>";
	private Logger logger = Logger.getLogger("Client Log");
	private DatagramSocket clientSocket;
	private final byte INITIALIZE=1, 
			FILE_READ=2, 
			FILE_WRITE=3, 
			FILE_DELETE=4, 
			FILE_INFO=5, 
			OPEN_DIR=6, 
			READ_DIR=7, 
			MAKE_DIR=8, 
			DELETE_DIR=9,
			MR=100;			
	
	public Client(String ip, String port, String masterIp, String masterPort, String logFile) throws ParseException {
		this.port=Integer.parseInt(port);
		this.ip=ip;
		this.masterIp=masterIp;
		this.masterPort=Integer.parseInt(masterPort);
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
	}

	public void run() throws IOException {
		InetAddress IPAddress = InetAddress.getByName(ip),
				masterAddress = InetAddress.getByName(masterIp);
		clientSocket = new DatagramSocket(port, IPAddress);
		Scanner sc=new Scanner(System.in);
		byte[] receiveData = new byte[1024];
		try{
			
			while (true){
				System.out.print(cmdStr);
				if (sc.hasNextLine()){
					String command=sc.nextLine();
					byte[] sendData=parseCommand(command);
					DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, masterAddress, masterPort);
					clientSocket.send(sendPacket);
					
					DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
					clientSocket.receive(receivePacket);
					receiveData=receivePacket.getData();
					parseResponse(receiveData);
				}
			}
		}
		finally{
			clientSocket.close();
		}
	}

	
	private void parseResponse(byte[] receiveData) {
		byte header=receiveData[0];
		switch (header){
		case INITIALIZE:
			break;
		case FILE_READ:			
			break;
		case FILE_WRITE:
			break;
		case FILE_DELETE:
			break;
		case FILE_INFO:
			break;
		case OPEN_DIR:
			break;
		case READ_DIR:
			break;
		case MAKE_DIR:
			break;
		case DELETE_DIR:
			break;	
		case INITIALIZE+10:
			logger.info("Successfully initialized!");
			cmdStr=ip+"_"+port+"/>";
			break;
		case FILE_WRITE+10:
			logger.info("File successfully written!");
			break;
		case FILE_DELETE+10:
			logger.info("File successfully deleted!");
			break;
		case OPEN_DIR+10:
//			if (currentFolder.endsWith("/") || currentFolder.endsWith("\\")){
//				currentFolder=currentFolder.substring(0, currentFolder.length()-1);
//			}
//			cmdStr=ip+"_"+port+"/"+currentFolder+"/>";
			break;
		case FILE_READ+10:			
		case FILE_INFO+10:
		case READ_DIR+10:
			byte[] arr=Arrays.copyOfRange(receiveData, 1, receiveData.length);
			String ls=new String (arr);
			ls=ls.trim();
			System.out.println(ls);
			break;
		case MAKE_DIR+10:
			logger.info("Directory created");
			break;
		case DELETE_DIR+10:
			break;	
		case INITIALIZE*-1:
			logger.info("Failed to initialize!");
			break;
		case FILE_READ*-1:
			logger.info("Failed to read the file! Make sure the filename and path is correct.");
			break;
		case FILE_WRITE*-1:
			logger.info("Failed to write the file! Make sure the filename and path is correct.");
			break;
		case FILE_DELETE*-1:
			logger.info("Failed to delete the file! Make sure the filename and path is correct.");
			break;
		case FILE_INFO*-1:
			logger.info("Failed to inspect the file! Make sure the filename and path is correct.");
			break;
		case OPEN_DIR*-1:
			logger.info("Failed to open directory! Make sure the name is correct.");
			break;
		case READ_DIR*-1:
			logger.info("Failed to read directory! Make sure the name is correct.");
			break;
		case MAKE_DIR*-1:
			logger.info("Failed to make directory! Make sure the path is correct.");
			break;
		case DELETE_DIR*-1:
			logger.info("Failed to delete directory! Make sure the name is correct.");
			break;	
		case MR + 10:
			logger.info("Map-Reduce task successfully finished!");
			break;
		default:
			logger.info("Unknown command "+header);
			break;
		}
	}

	private byte[] parseCommand(String command) throws IOException {
		String[] split=command.split(" ");
		byte[] result=new byte[length+22];
		if (split[0].equals("init")){
			result[0]=INITIALIZE;
		}
		else if (split[0].equals("fread")){
			result[0]=FILE_READ;
			String fname=split[1];
			result=merge(result, fname.getBytes());
		}
		else if (split[0].equals("fwrite") || split[0].equals("mr")){
			if (split[0].equals("fwrite"))
				result[0]=FILE_WRITE;
			else if (split[0].equals("mr"))
				result[0]=MR;
			String path=split[1];				
			File f=new File(path);
			FileReader fr=new FileReader(f);
			
			char[] cbuf=new char[length];
			fr.read(cbuf, 0, cbuf.length);
			String msg=new String(cbuf);
			fr.close();

			byte[] msgInBytes=msg.getBytes();
			result=merge(result, path.getBytes());
			for (int i=0; i<msgInBytes.length; i++){
				result[i+22]=msgInBytes[i];
			}
		}
		else if (split[0].equals("fdel")){
			result[0]=FILE_DELETE;
			String fname=split[1];
			result=merge(result, fname.getBytes());
		}
		else if (split[0].equals("finfo")){
			result[0]=FILE_INFO;
			String fname=split[1];
			result=merge(result, fname.getBytes());
		}
		else if (split[0].equals("cd")){
			result[0]=OPEN_DIR;
			String path=split[1];
			result=merge(result, path.getBytes());
		}
		else if (split[0].equals("ls")){
			result[0]=READ_DIR;
			if (split.length>1){
				String path=split[1];
				result=merge(result, path.getBytes());
			}
		}
		else if (split[0].equals("mkdir")){
			result[0]=MAKE_DIR;
			String path=split[1];
			result=merge(result, path.getBytes());
		}
		else if (split[0].equals("rmdir")){
			result[0]=DELETE_DIR;
			String path=split[1];
			result=merge(result, path.getBytes());
		}
		else if (split[0].equals("exit")){
			clientSocket.close();
			System.exit(0);
		}
		else{
			System.out.println("Unknown command! The commands supported: ");
		}
		return result;
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

	
}
