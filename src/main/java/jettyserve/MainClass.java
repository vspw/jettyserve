package jettyserve;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MainClass {
	protected static final Logger logger = LoggerFactory.getLogger(MainClass.class);

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		HttpJsonTransport httptrans= new HttpJsonTransport(args[0],Integer.parseInt(args[1]));
		httptrans.init();

		
	}

}
