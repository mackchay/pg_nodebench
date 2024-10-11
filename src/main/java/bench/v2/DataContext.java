package bench.v2;

import java.util.List;

public class DataContext {

	public String tableName;
	public List<Object> keyValues;
	
	public DataContext(String tableName, List<Object> keyValues) {
		this.tableName = tableName;
		this.keyValues = keyValues;
	}
}
