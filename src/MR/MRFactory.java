package MR;

public class MRFactory {

	public static IMapper getMapper(String name){
		if (name.equals("WORD COUNT")){
			return new WordCountMapper();
		}
		else return null;
	}
	
	public static IReducer getReducer(String name){
		if (name.equals("WORD COUNT")){
			return new WordCountReducer();
		}
		else return null;
	}
}
