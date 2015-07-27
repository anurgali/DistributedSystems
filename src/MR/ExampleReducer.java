package MR;

import java.util.List;
import java.util.Map;

import util.Config;

public class ExampleReducer implements IReducer {
	private int max=Integer.parseInt(Config.getString("max"));

	@Override
	public String reduce(Map<String, List<Object>> input) {
		StringBuilder output=new StringBuilder();
	//	int max=10;
		for (String key: input.keySet()){
			List<Object> list=input.get(key);
			int sum=0;
			
			for (Object o: list){
				
				sum+=(Integer)o;
			}
			if(max<=sum){
			output.append(key+"\t"+sum+"\n");
			max=sum;
			
			}
			
			
				
			}
		
		
		return output.toString();
	}

}
