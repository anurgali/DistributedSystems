package MR;

import java.io.File;

import DFS.Master;

public class Coordinator {

	private Master master;
	boolean isWorking=false;

	public Coordinator(Master master){
		this.master=master;
	}
	
	public void queue(Job job){
		isWorking=true;
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
		
		
	}
}
