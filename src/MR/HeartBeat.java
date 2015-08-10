package MR;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

import util.Config;

import DFS.IpPort;
import DFS.Master;

public class HeartBeat extends Thread{
	
	private boolean working=false;
	private DatagramSocket heartBeatSocket;
	private IpPort slave;
	private int port;
	private final byte HB=103;
	private int period;
	private Master master;

	public HeartBeat(IpPort slave, String masterIp, Master master){
		try {
			this.slave=slave;
			this.master=master;
			InetAddress IPAddress = InetAddress.getByName(masterIp);
			port=Integer.parseInt(Config.getString("heart_beat_port"));
			int timeout=Integer.parseInt(Config.getString("heart_beat_timeout"));
			period=Integer.parseInt(Config.getString("heart_beat_period"));
			heartBeatSocket = new DatagramSocket(port, IPAddress);
			heartBeatSocket.setSoTimeout(timeout);
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public void run() {
		while (working){
			try {
				byte[] sendData=new byte[1];
				sendData[0]=HB;
				DatagramPacket sendPacket = new DatagramPacket(sendData,
						sendData.length, slave.ip, port);
				heartBeatSocket.send(sendPacket);
				byte[] receiveData=new byte[2];
				DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
				heartBeatSocket.receive(receivePacket);
				receiveData = receivePacket.getData();
				if (receiveData[0]==HB){
					byte status=receiveData[1];
					switch (status){
					case 1://idle
						
						break;
					case 2://failed
						
						break;
					case 3://working
						
						break;
					case 4://complete
						
						break;
					default:
						break;
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				Thread.sleep(period);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
		}
		heartBeatSocket.close();
	}

	public boolean isWorking() {
		return working;
	}

	public void setWorking(boolean working) {
		this.working = working;
	}

}
