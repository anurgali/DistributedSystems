package MR;

public class MRFactory {

	public static IMapper getMapper(String name){
		if (name.equals("WORD_COUNT")){
			return new WordCountMapper();
		}
		else return new DummyMapper();
	}
	
	public static IReducer getReducer(String name){
		if (name.equals("WORD_COUNT")){
			return new WordCountReducer();
		}
		else return new DummyReducer();
	}
}
