package MR;

import java.util.List;
import java.util.Map;

public interface IReducer{

	public String reduce(Map<String, List<Object>> input);
}
