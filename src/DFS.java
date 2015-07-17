import java.io.IOException;
import java.text.ParseException;




public class DFS {

	public static void main(String args[]) throws ParseException, IOException {
		if (args.length>0){
			if (args[0].equals("-m")){
				String ipPort=args[1];
				String[] split=ipPort.split(":");
				String ip=split[0];
				String port=split[1];
				String slavesFile=args[2];
				String logFile=args[3];
				Master master = new Master(ip, port, slavesFile, logFile);
				master.run();
			}
			else if (args[0].equals("-c")){
				String ipPort=args[1];
				String[] split=ipPort.split(":");
				String ip=split[0];
				String port=split[1];
				split=args[2].split(":");
				String masterIp=split[0];
				String masterPort=split[1];
				String logFile=args[3];
				Client client=new Client(ip, port, masterIp, masterPort, logFile);
				client.run();
			}
			else if (args[0].equals("-s")){
				String ipPort=args[1];
				String[] split=ipPort.split(":");
				String ip=split[0];
				String port=split[1];
				Slave slave=new Slave(ip, port);
				slave.run();
			}
		}
		else {
			System.out.println("Usage: java DFS [-m|-s|-c] [ip:port] [masterIp:Port]* (for client) [slave file]* (for master) [log file]");
		}
	}
}
