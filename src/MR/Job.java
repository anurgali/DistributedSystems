package MR;

import java.io.File;

public class Job {

	private File input;
	

	public Job(File input){
		this.input=input;
	}


	public File getInput() {
		return input;
	}


	public void setInput(File input) {
		this.input = input;
	}
	
}
