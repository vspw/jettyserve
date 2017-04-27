package jettyserve;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean; 

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse; 
import javax.servlet.http.HttpServletRequest; 



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasException;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.notification.MessageDeserializer;
import org.apache.atlas.notification.hook.HookMessageDeserializer;
import org.apache.atlas.notification.hook.HookNotification;
import org.apache.atlas.utils.AuthenticationUtil;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONException;
import org.eclipse.jetty.server.Handler; 
import org.eclipse.jetty.server.Request; 
import org.eclipse.jetty.server.ServerConnector; 
import org.eclipse.jetty.server.Server; 
import org.eclipse.jetty.server.handler.AbstractHandler; 

public class HttpJsonTransport { 

	private static Logger LOG = LoggerFactory.getLogger(HttpJsonTransport.class); 
	private Server server; 
	private static ObjectMapper MAPPER = new ObjectMapper(); 
	private static String HOST;
	private static int PORT;
	private final AtomicBoolean RUNNING = new AtomicBoolean(false); 


	public HttpJsonTransport(String hostname,int port){ 
		this.HOST = hostname; 
		this.PORT=port;
	} 

	public void init(){ 
		server = new Server(); 
		ServerConnector s = new ServerConnector(server); 
		s.setHost(HOST); 
		s.setPort(PORT); 
		server.addConnector(s); 
		//server.setDumpBeforeStop(true); 
		server.setHandler(getHandler()); 
		try { 
			server.start(); 
			RUNNING.set(true); 
		} catch (Exception e) { 
			throw new RuntimeException(e); 
		} 
	} 

	public void shutdown() { 
		try { 
			server.stop(); 

			RUNNING.set(false); 
		} catch (Exception e) { 
			e.printStackTrace(); 
			throw new RuntimeException(e); 
		} 
	} 

	private Handler getHandler(){ 
		AbstractHandler handler = new AbstractHandler() { 
			public void handle(String target, Request request, HttpServletRequest servletRequest, 
					HttpServletResponse response) throws IOException, ServletException {
				int MAX_BODY_SIZE = 10240; // Can only handle bodies of up to 10240 bytes.
				byte[] b = new byte[MAX_BODY_SIZE];
				int offset = 0;
				int numBytesRead;
				try (ServletInputStream is = request.getInputStream()) {
					while ((numBytesRead = is.read(b, offset, MAX_BODY_SIZE - offset)) != -1) {
						offset += numBytesRead;
					}
					LOG.info("Byte offset read:"+offset);
				}
				LOG.info("200 status code");
				response.setContentType(request.getContentType());
				response.setStatus(200);
				LOG.info("setting char encoding");
				response.setCharacterEncoding(request.getCharacterEncoding());
				LOG.info("setting contenet lentgh");
				response.setContentLength(request.getContentLength());
				LOG.info("setting os write");

				try (ServletOutputStream os = response.getOutputStream()) {
					// LOG.info("Second Character in request:"+b[1]);
					LOG.info(Arrays.toString(b));
					os.write(b, 0, offset);   
				}
				response.getOutputStream().flush();

				String messageResponse = new String(b,0,offset, "UTF-8");;
				LOG.info("Done writing:"+messageResponse);
				
				try {
					postAtlas(messageResponse);
				} catch (AtlasException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (AtlasServiceException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			} 
		}; 
		return handler; 
	}
	
	private void postAtlas(String strJsonMessage) throws AtlasException, IOException, JSONException, AtlasServiceException
	{
		// on the server side. probably have similar behavior
		LOG.info("HWX:Serverside messageServe:" +strJsonMessage);
		LOG.info("HWX:ServerSide: Deserializing received message");
		if( !isJSONValid(strJsonMessage))
		{
			LOG.warn("HWX:ServerSide: Invalid Json!!:"+strJsonMessage + " with lenght " +strJsonMessage.length());
		}
		else
		{
			LOG.info("HWX:ServerSide: ValidJson"+strJsonMessage + " with lenght " +strJsonMessage.length());
		}
		final MessageDeserializer<HookNotification.HookNotificationMessage> deserializer=new HookMessageDeserializer();
		HookNotification.HookNotificationMessage des_hookNotifMsg=deserializer.deserialize(strJsonMessage);
		LOG.debug("HWX:ServerSide: Deserilizer class: "+deserializer.getClass().getName());
		LOG.debug("HWX:ServerSide: deserialized msg type: {} \n and msg: {}" +des_hookNotifMsg.getType().toString(), des_hookNotifMsg);

		LOG.info("HWX:ServerSide: Casting HookNotificationMessage into EntityCreateRequest");
		HookNotification.EntityCreateRequest objEntityCreateRequest =
				(HookNotification.EntityCreateRequest) des_hookNotifMsg;
		LOG.debug("HWX:ServerSide: EntityCreateRequest: "+objEntityCreateRequest.toString());

		LOG.debug("HWX:ServerSide: Creating Atlas Client to POST requests ");
		Configuration atlasConf = ApplicationProperties.get();
		String ATLAS_ENDPOINT = "atlas.rest.address";
		String[] atlasEndpoint = atlasConf.getStringArray(ATLAS_ENDPOINT);
		String DEFAULT_DGI_URL = "http://localhost:21000/";
		if (atlasEndpoint == null || atlasEndpoint.length == 0){
			atlasEndpoint = new String[] { DEFAULT_DGI_URL };
		}
		AtlasClient atlasClient;

		if (!AuthenticationUtil.isKerberosAuthenticationEnabled()) {
			//String[] basicAuthUsernamePassword = AuthenticationUtil.getBasicAuthenticationInput();
			atlasClient = new AtlasClient(atlasEndpoint, new String[]{"T93KOAI", "Monday14"});
		} else {
			UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
			atlasClient = new AtlasClient(ugi, ugi.getShortUserName(), atlasEndpoint);
		}
		LOG.info("HWX:ServerSide: Creating Entity using Atlas Client");
		LOG.debug("HWX:ServerSide: createRequestGetEntities: "+objEntityCreateRequest.getEntities());
		//JSONArray entityArray = new JSONArray(createRequest.getEntities().size());
		atlasClient.createEntity(objEntityCreateRequest.getEntities());
	}
	  public static boolean isJSONValid(String jsonInString) {
	      try {
	    	  
	          new Gson().fromJson(jsonInString, Object.class);
	          return true;
	      } catch(com.google.gson.JsonSyntaxException ex) { 
	          return false;
	      }
	  }
}