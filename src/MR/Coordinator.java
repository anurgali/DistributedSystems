package MR;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;

import DFS.IpPort;
import DFS.Master;

public class Coordinator {
    private static File myFile;
	private Master master;
	boolean isWorking=false;
	private String fullPath;
	private IpPort _clientAddress;
	private Map<String, List<Object>> oneBigMap=new TreeMap<String, List<Object>>();
	private int countMaps, countReducers, linesForSlaves;

	public Coordinator(Master master){

        this.master=master;
	}
	
	//1. get input of the file and split into pieces <<<< Askhat + DONE
	
	//2. send each piece to Master <<<<Askhat +DONE, I hope
	
	//3. Master sends to slaves the Mapper work via RMI and sends back to Coordinator the result <<<Albert
	
	//4. Coordinator waits for every Mapper piece to finish <<< Albert
	
	//5. All pieces received: Key1 -> Value1, Key2 -> Value2, Key1 -> Value3   <<<Askhat
	// shuffle & merge & sort: {Key1 -> Value1, Value3}   {Key2 -> Value2}
	// Send these key-value pairs to Master
	
	//6. Master sends to slaves the Reducer work via RMI and waits for reply  << Albert
	//each slave writes down the file with the result and sends back the response.
	
	//7. Coordinator waits until all tasks are finished and once they finish, it displays "Success"
	//after that it can start a new Job. <<<Askhat
	public void queue(String fullPath, IpPort _clientAddress, String text) throws IOException{
		if (isWorking)
			return;
		countMaps=0;
		oneBigMap.clear();
		this.fullPath=fullPath;
		this._clientAddress=_clientAddress;
		isWorking=true;
		
		int numberOfSlaves=master.get_slaves().size();
		int numberOfLines=0;
		numberOfLines=countLines(text);
		linesForSlaves=numberOfLines/numberOfSlaves;
        //read file
		StringBuilder textPart=new StringBuilder();
        Scanner sc=new Scanner(text);
        try {
            int groupNumber = 0, count = 0;
            while (sc.hasNextLine()) {
            	String line=sc.nextLine().trim();
            	if (line.length()==0)
            		continue;
            	count++;
            	textPart.append(line+'\n');
            	if(count==linesForSlaves){
                    if(groupNumber==numberOfSlaves-1){
                    	while (sc.hasNextLine()){
                    		line=sc.nextLine();
                        	count++;
                        	textPart.append(line+'\n');
                    	}
                        master.sendToMapper(groupNumber, textPart.toString(), fullPath);
                    }
                    else {
                        master.sendToMapper(groupNumber, textPart.toString(), fullPath);
                        groupNumber++;
                        count = 0;
                        textPart = new StringBuilder();
                    }
                }                
            }
        }finally {
            sc.close();
        }

	
	}
	
	public int countLines(String msg) {
	    Scanner sc=new Scanner(msg);
	    int count=0;
	    while (sc.hasNextLine()){
	    	String l=sc.nextLine().trim();
	    	if (l.length()>0)
	    		count++;
	    }
	    sc.close();
	    return count;
	}

	public void mergeMaps(Map<String, List<Object>> output) {
		for (String key: output.keySet()){
			List<Object> values = output.get(key);
			if (oneBigMap.containsKey(key)){
				oneBigMap.get(key).addAll(values);
			}
			else{
				oneBigMap.put(key, values);
			}
		}
		int numberOfSlaves=master.get_slaves().size();
		countMaps++;
		if (countMaps==numberOfSlaves){
			countMaps=0;
			int keysPerSlave=oneBigMap.size()/master.get_slaves().size();
			int count=0, groupNumber=0;
			for (String key: oneBigMap.keySet()){
				List<Object> values = oneBigMap.get(key);
				try {
					master.sendToReducer(groupNumber, key, values, fullPath, _clientAddress);
					countReducers++;
				} catch (IOException e) {
					e.printStackTrace();
				}
				count++;
				if (count==keysPerSlave){
					count=0;
					groupNumber++;
				}
				if (groupNumber>master.get_slaves().size()-1){
					groupNumber=master.get_slaves().size()-1;
				}
			}
		}
	}

	public void taskCount() {
		countReducers--;
		if (countReducers==0){
			master.sendSuccess(true, fullPath, _clientAddress);
		}
	}

	
}
