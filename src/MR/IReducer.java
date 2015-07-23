package MR;

import java.util.List;
import java.util.Map;

public interface IReducer{

	public void reduce(Map<String, List<Object>> input, Object output);
}
