package MR;

public class MRFactory {

	public static IMapper getMapper(String name){
		if (name.equals("WORD_COUNT")){
			return new WordCountMapper();
		}
		else if(name.equals("EXAMPLE_TWO")){
			return new ExampleMapper();
		} 
		else return new DummyMapper();
	}
	
	public static IReducer getReducer(String name){
		if (name.equals("WORD_COUNT")){
			return new WordCountReducer();
		}
		else if (name.equals("EXAMPLE_TWO")){
			return new ExampleReducer();
		}
		else return new DummyReducer();
	}
}
