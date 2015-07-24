package MR;

import java.util.List;
import java.util.Map;

public class Reducer implements IReducer {

	@Override
	public String reduce(Map<String, List<Object>> input) {
		StringBuilder output=new StringBuilder();
		for (String key: input.keySet()){
			List<Object> list=input.get(key);
			int sum=0;
			for (Object o: list){
				sum+=(Integer)o;
			}
			output.append(key+"\t"+sum+"\n");
		}
		return output.toString();
	}

}
