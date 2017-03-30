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
import org.codehaus.jackson.map.ObjectMapper; 
import org.eclipse.jetty.server.Handler; 
import org.eclipse.jetty.server.Request; 
import org.eclipse.jetty.server.ServerConnector; 
import org.eclipse.jetty.server.Server; 
import org.eclipse.jetty.server.handler.AbstractHandler; 
 
public class HttpJsonTransport { 
 
  private static Logger LOGGER = LoggerFactory.getLogger(HttpJsonTransport.class); 
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
          int MAX_BODY_SIZE = 1024; // Can only handle bodies of up to 1024 bytes.
          byte[] b = new byte[MAX_BODY_SIZE];
          int offset = 0;
          int numBytesRead;
          try (ServletInputStream is = request.getInputStream()) {
              while ((numBytesRead = is.read(b, offset, MAX_BODY_SIZE - offset)) != -1) {
                  offset += numBytesRead;
              }
          System.out.println("Byte offset read:"+offset);
          }
System.out.println("200 status code");
          response.setContentType(request.getContentType());
          response.setStatus(200);
          System.out.println("setting char encoding");
          response.setCharacterEncoding(request.getCharacterEncoding());
          System.out.println("setting contenet lentgh");
          response.setContentLength(request.getContentLength());
          System.out.println("setting os write");

          try (ServletOutputStream os = response.getOutputStream()) {
        	 // System.out.println("Second Character in request:"+b[1]);
        	  System.out.println(Arrays.toString(b));
              os.write(b, 0, offset);   
          }
         response.getOutputStream().flush();
          
          System.out.println("Done writing");
         
      } 
    }; 
    return handler; 
  } 
}