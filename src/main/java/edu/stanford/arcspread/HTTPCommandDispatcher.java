package edu.stanford.arcspread;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Vector;

public class HTTPCommandDispatcher implements HttpConstants,
		PropertyChangeListener {

	// For initiating shutdown:
	private static boolean keepRunning = true; 
	
	/* timeout on client connections */
	static int CLIENT_CONNECTION_TIMEOUT = 0;
	/* max # worker threads */
	static int NUM_WORKERS = 5;
	static int PORT = 8080;

	final static int HTTP_TOKEN_POS_COMMAND = 0; // e.g. ["GET",
	// "usrCmd?foo=10&bar=baz"]
	final static int HTTP_TOKEN_POS_ALL_ARGS = 1;
	final static int HTTP_TOKEN_POS_USER_COMMAND = 0; // e.g. ["usrCmd",
	// "foo=10&bar=baz]
	final static int HTTP_TOKEN_POS_USER_ARGS = 1;
	final static int HTTP_TOKEN_POS_KEY = 0; // e.g. ["foo",10]
	final static int HTTP_TOKEN_POS_VALUE = 1;

	final static String HTTP_TOKEN_SEP = " ";
	final static String HTTP_COMMAND_SEP = "\\?";
	final static String HTTP_ARGS_SEP = "&";
	final static String HTTP_ATTR_VAL_SEP = "=";

	final static String HTTP_RESPONSE_OPENING = "<html><body><h2>";
	final static String HTTP_RESPONSE_HEADER_CLOSING = "</h2>";
	final static String HTTP_RESPONSE_CLOSING = "</body></html>";
	final static int HTTP_RESPONSE_OVERHEAD = HTTP_RESPONSE_OPENING.length()
			+ HTTP_RESPONSE_HEADER_CLOSING.length()
			+ HTTP_RESPONSE_CLOSING.length();

	private static HTTPCommandDispatcher soleInstance = null;
	private static SocketListener sl;

	/* Where worker threads stand idle */
	protected Vector<Worker> threads = new Vector<Worker>();
	//protected static PropertyChangeSupport pcs = MediaController.getPropertyChangeSupport();
	PropertyChangeSupport pcs = null;
		
	String webPage = null;

	public static HTTPCommandDispatcher getInstance() {
		if (soleInstance != null)
			return soleInstance;
		soleInstance = new HTTPCommandDispatcher(); 
		return soleInstance;
	}

	private HTTPCommandDispatcher() {
		initDataStructs();
		// The -1 causes the default port to be used:
		sl= new SocketListener(-1);
		@SuppressWarnings("unused")
		PropertyChangeSupport pcs = new PropertyChangeSupport(this);

		new Thread(sl).start();
	}

	public void initDataStructs() {
		pcs = new PropertyChangeSupport(this);
		// ***************
		// addListener("myCommand", this);
		// setWebPage("<html><body><h2>Testing it</h2>\nThis is a test.\nFor a page, that is.</body></html>");
		// ***************
	}

	public void propertyChange(PropertyChangeEvent arg0) {
		// We don't listen to any property changes
	}

	public void addListener(String command, PropertyChangeListener listener) {
		pcs.addPropertyChangeListener(command, listener);
	}

	public void removeListener(PropertyChangeListener listener) {
		pcs.removePropertyChangeListener(listener);
	}

	public void setWebPage(String page) {
		webPage = page;
	}

	public void setWebPage(File webPageFile) throws IOException {

		webPage = "";
		if (!webPageFile.exists())
			throw new FileNotFoundException("Web page file '"
					+ webPageFile.getPath() + "' does not exist.");
		if (!webPageFile.canRead())
			throw new FileNotFoundException(
					"File '"
							+ webPageFile
							+ "' exists, but is not readable by the command dispatcher.");

		FileReader fr = new FileReader(webPageFile);
		BufferedReader br = new BufferedReader(fr);
		String oneLine;
		while ((oneLine = br.readLine()) != null) {
			webPage += oneLine;
		}
		fr.close();
	}

	public String getWebPage() {
		return webPage;
	}
	
	
	private class SocketListener implements Runnable {
		
		int port = PORT;
		public SocketListener(int thePort) {
			if (thePort > 0)
				port = thePort;
		}

	public void dispatchLoop() {

		/* start worker threads */
		for (int i = 0; i < NUM_WORKERS; ++i) {
			Worker w = new Worker();
			(new Thread(w, "worker #" + i)).start();
			threads.addElement(w);
		}
		ServerSocket ss = null;
		try {
			ss = new ServerSocket(port);
		} catch (IOException e) {
			e.printStackTrace();
		}
		while (keepRunning) {
			Socket s = null;
			try {
				s = ss.accept();
			} catch (IOException e) {
				e.printStackTrace();
			}
			Worker w = null;
			synchronized (threads) {
				if (threads.isEmpty()) {
					Worker ws = new Worker();
					ws.setSocket(s);
					(new Thread(ws, "additional worker")).start();
				} else {
					w = (Worker) threads.elementAt(0);
					threads.removeElementAt(0);
					w.setSocket(s);
				}
			}
		} // while (true)
	} // end dispatchLoop()

	public void run() {
		dispatchLoop();
		
	}
	} // end class SocketListener

	// class Worker extends HTTPCommandDispatcher implements HttpConstants,
	// Runnable {

	class Worker implements HttpConstants, Runnable {

		final static int BUF_SIZE = 2048;
		final byte[] EOL = { (byte) '\r', (byte) '\n' };

		/* buffer to use for requests */
		byte[] buf;
		/* Socket to client we're handling */
		private Socket sock;

		Worker() {
			buf = new byte[BUF_SIZE];
			sock = null;
		}

		synchronized void setSocket(Socket s) {
			this.sock = s;
			notify();
		}

		public synchronized void run() {
			while (true) {
				if (sock == null) {
					/* nothing to do */
					try {
						wait();
					} catch (InterruptedException e) {
						/* should not happen */
						continue;
					}
				}
				try {
					handleClient();
				} catch (Exception e) {
					e.printStackTrace();
				}
				/*
				 * go back in wait queue if there's fewer than NUM_WORKERS
				 * connections.
				 */
				sock = null;
				Vector<Worker> pool = threads;
				synchronized (pool) {
					if (pool.size() >= HTTPCommandDispatcher.NUM_WORKERS) {
						/* too many threads, exit this one */
						return;
					} else {
						pool.addElement(this);
					}
				}
			}
		}

		void handleClient() throws IOException {

			InputStream is = new BufferedInputStream(sock.getInputStream());
			PrintStream ps = new PrintStream(sock.getOutputStream());
			String httpMsg = null;
			/*
			 * we will only block in read for this many milliseconds before we
			 * fail with java.io.InterruptedIOException, at which point we will
			 * abandon the connection.
			 */
			sock.setSoTimeout(HTTPCommandDispatcher.CLIENT_CONNECTION_TIMEOUT);

			// Zero the read buffer:
			for (int i = 0; i < buf.length; i++)
				buf[i] = 0;
			try {
				/*
				 * We only support HTTP GET/HEAD, and don't support any fancy
				 * HTTP options, so we're only interested really in the first
				 * line.
				 */
				int numCharsRead = 0, actuallyRead = 0;
				// Read one line from client connection:
				outerloop: while (numCharsRead < BUF_SIZE) {
					actuallyRead = is.read(buf, numCharsRead, BUF_SIZE
							- numCharsRead);
					if (actuallyRead == -1) {
						/* EOF */
						return;
					}
					int i = numCharsRead;
					numCharsRead += actuallyRead;
					for (; i < numCharsRead; i++) {
						if (buf[i] == (byte) '\n' || buf[i] == (byte) '\r') {
							// Read one line:
							break outerloop;
						}
					}
				}

				/*
				 * Get something like: GET /play?file=help.mp3&volume=14
				 * HTTP/1.1 Host: 192.168.0.36:8080 User-Agent: Mozilla/5.0
				 * (Windows; U; Windows NT 6.0; en-US; rv:1.9.1.7)
				 * Gecko/20091221 Firefox/3.5.7 GTB6 (.NET CLR 3.5.30729)
				 * Accept:
				 * text/html,application/xhtml+xml,application/xml;q=0.9,...
				 * Accept-Language: en-us,en;q=0.5 Accept-Encoding: gzip,deflate
				 * Accept-Charset: ISO-8859-1,utf-8;q=0.7,*;q=0.7 Keep-Alive:
				 * 300 Connection: keep-alive
				 */
				httpMsg = new String(buf, 0, numCharsRead);

				/* Are we doing a GET or just a HEAD? */
				boolean doingGet;

				String[] urlComponents = httpMsg.split(HTTP_TOKEN_SEP); // " "
				String urlCommand = urlComponents[HTTP_TOKEN_POS_COMMAND];
				if (urlCommand.equals("GET"))
					doingGet = true;
				else if (urlCommand.equals("HEAD"))
					doingGet = false;
				else {
					/* we don't support this method */
					ps.print("HTTP/1.0 " + HTTP_BAD_METHOD
							+ " unsupported method type: ");
					ps.print(urlCommand);
					ps.write(EOL);
					ps.flush();
					sock.close();
					return;
				}

				// Get a command object for the property change firing:
				Command command = new Command();

				// Get the "myCommand?foo=10&bar=baz&..." part:
				String allArgs = urlComponents[HTTP_TOKEN_POS_ALL_ARGS];
				// Get ["myCommand", "foo=10&bar=baz&..."]:
				String[] userCommandAndArgs = allArgs.split(HTTP_COMMAND_SEP); // "?"
				// Get "myCommand":
				command.setCommandName(userCommandAndArgs[HTTP_TOKEN_POS_USER_COMMAND]
				                                          .substring(1));
				ArrayList<String> parmVals = null;
				// Add any parameter key/value pairs to the command object;
				// If the URL command portion is mal-formed the parseCommandArgs()
				// function will return null (if there just aren't any parms,
				// the return will be an empty ArrayList:
				parmVals = parseCommandArgs(command, userCommandAndArgs);

				if (parmVals == null) {
					String cmdName = 
						command.getCommandName().isEmpty() ? 
								"[noCmdName]" : command.getCommandName();    
					sendNoListenerWarning(
							ps, 
							cmdName,
							parmVals,
							HTTP_BAD_REQUEST,
							"Malformed MediaController Request",
							"MediaController command was malformed: ");
					return;
				}

				if (command.getCommandName().isEmpty()) {
					// No command in the URL, just send the Web page,
					// if one was defined:
					if (doingGet)
						sendOK(ps);
					return;
				}
				else if (!pcs.hasListeners(command.getCommandName()) && doingGet) {
					// URL contains a command, but nobody is listening to it:
					sendNoListenerWarning(
							ps, 
							command.getCommandName(), 
							parmVals,
							HTTP_BAD_METHOD,
							"No Command Handler Running",
							"The MediaController server has no command handler running for: ");
					return;
				}
				pcs.firePropertyChange(command.getCommandName(), null, command);
				if (doingGet)
					sendOK(ps);

				// 
			} finally {
				sock.close();
			}
		}

		/**
		 * Add the parm key/val instance vars to the command obj.
		 * @param command
		 * @param userCommandAndArgs
		 * @return null if passed-inuserCommandAndArgs was malformed.
		 * 		   Else ArrayList of parameter values for use in
		 *         possibly necessary error messages later on. If
		 *         no parameters are included, the ArrayList will 
		 *         be empty. 
		 */
		private ArrayList<String> parseCommandArgs(Command command,
				String[] userCommandAndArgs) {
			
			ArrayList<String> parmVals = new ArrayList<String>();
			
			if (!sanityCheckUrlCommand(userCommandAndArgs))
				return null;
			
			// If no parameters present, return the empty parmVals
			// ArrayList:
			if (userCommandAndArgs.length < 2)
				return parmVals;
			
			String allUserArgs = userCommandAndArgs[HTTP_TOKEN_POS_USER_ARGS];
			// Get ["foo=10", "bar=baz", ...]:
			String[] argUserVals = allUserArgs.split(HTTP_ARGS_SEP);

			// Go through all the foo=10, bar=baz, ... elements
			// and get the keys and values:
			for (String argVal : argUserVals) {
				String[] keyVal = argVal.split(HTTP_ATTR_VAL_SEP);
				if (keyVal.length != 2) {
					TimeGate.log(
							"HTTPDispatcher: argument name without argument value. Command: " + 
							command.getCommandName() +
							". Argument without value: " +
							(keyVal.length == 1 ? keyVal[0] : "none."));
					return null;
				}
				String key = keyVal[HTTP_TOKEN_POS_KEY];
				String val = keyVal[HTTP_TOKEN_POS_VALUE];
				command.put(key, val);
				// Collect the parameter values for better error messages
				// later:
				parmVals.add(val);
			}
			return parmVals;
		}


		/**
		 * Given an array of the partially parsed command portion
		 * of the URL, return true if the info makes sense. Examples
		 * for healthy:
		 *    ["/"],                ;; ok
		 *    ["/ping"],            ;; ok
		 *    ["/", "ping"],        ;; bad
		 *    ["/play", "file=foo"] ;; ok
		 * @param userCommandAndArgs Array of strings w/ partially parsed URL command portion
		 * @return true if command seems ok so far.
		 */
		private boolean sanityCheckUrlCommand(String[] userCommandAndArgs) {
			if ((userCommandAndArgs.length > 1) &&
				(userCommandAndArgs[0].length() < 2))
				return false;
			
			return true;
		}
		
		private String getDateTime() {
			Calendar cal = Calendar.getInstance();
			SimpleDateFormat dateFormat = new SimpleDateFormat(
					"dw, dd MM yyyy hh:mm:ss zzz");
			return dateFormat.format(cal.getTime());
		}

		private void sendResponse(PrintStream ps, int retCode,
				String httpShortMsg, String htmlBody) throws IOException {

			ps.print("HTTP/1.1 " + retCode + " " + httpShortMsg);
			ps.write(EOL);
			if (htmlBody == null) {
				// End of return header:
				ps.write(EOL);
				return;
			}
			// Was some kind of error, or all OK, and a web page will be
			// returned;
			// generate an html error response page:
			ps.print(getDateTime());
			ps.write(EOL);
			ps.print("Content-Type: text/html");
			ps.write(EOL);
			int contentLen = 0;
			contentLen += HTTP_RESPONSE_OVERHEAD + htmlBody.length();
			ps.print("Content-Length: " + contentLen);
			ps.write(EOL);
			ps.write(EOL);
			ps.print(htmlBody);
			ps.write(EOL);
		}

		void sendOK(PrintStream ps) throws IOException {
			sendResponse(ps, HTTP_OK, " OK", webPage);
		}

		void sendNoListenerWarning(
				PrintStream ps, 
				String commandName,
				ArrayList<String> parmVals,
				int HTTPErrorCode,
				String shortDescription,
				String detailedDescription) throws IOException {
			
			// Build a string "commandName(parm1Name, parm2Name,...)":
			String commandString = constructCommandString(commandName,
					parmVals);
			
			String htmlBody = "<html><page><h2>" + shortDescription + "</h2>\n"
					+ detailedDescription
					+ commandString + ".\n</body></html>";
			sendResponse(ps, HTTPErrorCode, shortDescription, htmlBody);
		}

		/**
		 * Build a string that reflects the URL's command portion
		 * expressed as a function call:
		 * Example: "play(10, bluebell)"
		 * @param commandName Name of the MediaController command
		 * @param parmValues
		 * @return Command string in the syntax of a function call.
		 */
		private String constructCommandString(String commandName,
				ArrayList<String> parmVals) {
			String commandString = commandName + "(";
			if (parmVals != null)
				for (String parmName : parmVals)
					commandString += parmName + ",";
			// Strip any trailing comma and close the parens:
			if (commandString.endsWith(","))
				commandString = commandString.substring(0, commandString
						.length() - 1)
						+ ")";
			else
				commandString += ")";
			return commandString;
		}
	} // end Worker
	
	public void shutdown() {
		keepRunning = false;
	}
	
} // end HTTPCommandDispatcher

interface HttpConstants {
	/** 2XX: generally "OK" */
	public static final int HTTP_OK = 200;
	public static final int HTTP_CREATED = 201;
	public static final int HTTP_ACCEPTED = 202;
	public static final int HTTP_NOT_AUTHORITATIVE = 203;
	public static final int HTTP_NO_CONTENT = 204;
	public static final int HTTP_RESET = 205;
	public static final int HTTP_PARTIAL = 206;

	/** 3XX: relocation/redirect */
	public static final int HTTP_MULT_CHOICE = 300;
	public static final int HTTP_MOVED_PERM = 301;
	public static final int HTTP_MOVED_TEMP = 302;
	public static final int HTTP_SEE_OTHER = 303;
	public static final int HTTP_NOT_MODIFIED = 304;
	public static final int HTTP_USE_PROXY = 305;

	/** 4XX: client error */
	public static final int HTTP_BAD_REQUEST = 400;
	public static final int HTTP_UNAUTHORIZED = 401;
	public static final int HTTP_PAYMENT_REQUIRED = 402;
	public static final int HTTP_FORBIDDEN = 403;
	public static final int HTTP_NOT_FOUND = 404;
	public static final int HTTP_BAD_METHOD = 405;
	public static final int HTTP_NOT_ACCEPTABLE = 406;
	public static final int HTTP_PROXY_AUTH = 407;
	public static final int HTTP_CLIENT_TIMEOUT = 408;
	public static final int HTTP_CONFLICT = 409;
	public static final int HTTP_GONE = 410;
	public static final int HTTP_LENGTH_REQUIRED = 411;
	public static final int HTTP_PRECON_FAILED = 412;
	public static final int HTTP_ENTITY_TOO_LARGE = 413;
	public static final int HTTP_REQ_TOO_LONG = 414;
	public static final int HTTP_UNSUPPORTED_TYPE = 415;

	/** 5XX: server error */
	public static final int HTTP_SERVER_ERROR = 500;
	public static final int HTTP_INTERNAL_ERROR = 501;
	public static final int HTTP_BAD_GATEWAY = 502;
	public static final int HTTP_UNAVAILABLE = 503;
	public static final int HTTP_GATEWAY_TIMEOUT = 504;
	public static final int HTTP_VERSION = 505;
}
