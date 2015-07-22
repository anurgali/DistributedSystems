package MR;

import java.util.Map;

public interface IReducer{

	public void reduce(Map<Object, Object> input, Object output);
}
