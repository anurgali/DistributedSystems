package MR;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class WordCountMapper implements IMapper {

	@Override
	public void map(String fileName, String text,
			Map<String, List<Object>> output) {
		text=text.toLowerCase();
		text=text.replaceAll("[^A-Za-z]", " ");
		Scanner sc=new Scanner(text);
		while (sc.hasNext()){
			String word=sc.next().trim();
			if (output.containsKey(word)){
				output.get(word).add(1);
			}
			else{
				List<Object> list=new ArrayList<Object>();
				list.add(1);
				output.put(word, list);
			}
		}
	}

}
