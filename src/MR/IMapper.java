package MR;

import java.util.List;
import java.util.Map;

public interface IMapper {

	public void map(String fileName, String text, Map<String, List<Object>> output);
}
