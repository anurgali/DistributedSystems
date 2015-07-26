package MR;

import java.util.List;
import java.util.Map;

public class DummyReducer implements IReducer {

	@Override
	public String reduce(Map<String, List<Object>> input) {
		return "";
	}

}
