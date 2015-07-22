package MR;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;

import DFS.Master;

public class Coordinator {

	private Master master;
	boolean isWorking=false;

	public Coordinator(Master master){
		this.master=master;
	}
	
	//1. get input of the file and split into pieces <<<< Askhat
	
	//2. send each piece to Master <<<<Askhat
	
	//3. Master sends to slaves the Mapper work via RMI and sends back to Coordinator the result <<<Albert
	
	//4. Coordinator waits for every Mapper piece to finish <<< Albert
	
	//5. All pieces received: Key1 -> Value1, Key2 -> Value2, Key1 -> Value3   <<<Askhat
	// shuffle & merge & sort: {Key1 -> Value1, Value3}   {Key2 -> Value2}
	// Send these key-value pairs to Master
	
	//6. Master sends to slaves the Reducer work via RMI and waits for reply  << Albert
	//each slave writes down the file with the result and sends back the response.
	
	//7. Coordinator waits until all tasks are finished and once they finish, it displays "Success"
	//after that it can start a new Job. <<<Askhat
	public void queue(Job job) throws IOException{
		isWorking=true;
		
		int numberOfSlaves=master.get_slaves().size();
		int numberOfLines=0;
		try {
			numberOfLines=countLines(job.getInput().getPath());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		int linesForSlaves=numberOfLines/numberOfSlaves;
		
		//read file
		File f=job.getInput();
		StringBuilder text=new StringBuilder();
		text.append("");
		/*  int groupNumber=0, count=0;
		 * while file readline
		 * {
		 * 		count++;
		 * 		
		 * 		text.append(readLine());
		 * 		if (count==linesForSlaves){
		 * 			master.sendToMapper(0, text.toString());
		 * 			count=0;
		 * 			text.clear();
		 * 			groupNumber++;
		 * 		}
		 * }
		 */
		//take N number of Lines and send each to Master
		//master.sendToMapper(0, text.toString());
		
		
		//wait for all pieces to finish
	
	}
	
	// taken from: http://stackoverflow.com/questions/453018/number-of-lines-in-a-file-in-java
	public int countLines(String filename) throws IOException {
	    InputStream is = new BufferedInputStream(new FileInputStream(filename));
	    try {
	        byte[] c = new byte[1024];
	        int count = 0;
	        int readChars = 0;
	        boolean empty = true;
	        while ((readChars = is.read(c)) != -1) {
	            empty = false;
	            for (int i = 0; i < readChars; ++i) {
	                if (c[i] == '\n') {
	                    ++count;
	                }
	            }
	        }
	        return (count == 0 && !empty) ? 1 : count;
	    } finally {
	        is.close();
	    }
	}
}
