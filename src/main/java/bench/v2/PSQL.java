package bench.v2;

import bench.V2;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ProcessBuilder.Redirect;

public class PSQL {
	
	public static void executeFile(String filename, Integer hostNum) {
		Database ds = V2.db;
		executeFile(filename, ds.userName, ds.passwd, ds.dbName, hostNum);
	}
	
	public static void executeFile(String filename, String userName, String password, String database, Integer hostNum) {
		Database ds = V2.db;
		String conn = "postgresql://" + userName +  ":" + password + "@"  + ds.hosts[hostNum] + ":" + ds.port + "/" + database;
		
		InputStream is = PSQL.class.getClassLoader().getResourceAsStream(filename);


        ProcessBuilder builder = new ProcessBuilder("psql", conn, (is != null) ? "--file=-": "--file=" + filename);
		builder.redirectOutput(Redirect.INHERIT);
		builder.redirectError(Redirect.INHERIT);
		
		try {
			Process psql = builder.start();

			if (is != null) {
				OutputStream os = psql.getOutputStream();
				
				int len;
				byte b[] = new byte[4096];
				while((len = is.read(b)) != -1) {
					os.write(b, 0, len);
				}
				os.flush();
				os.close();
			}
			
			int exitCode = psql.waitFor();
			if (exitCode != 0) 
				throw new RuntimeException("PSQL exited with code: " + exitCode);
		} catch (IOException | InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
}
