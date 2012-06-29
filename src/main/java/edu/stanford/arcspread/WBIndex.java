package edu.stanford.arcspread;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.DataFormatException;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

public class WBIndex {
	
	SQLiteConnection indexDB = null;
	SQLiteStatement  urlDatesQuery = null;
	SQLiteStatement  urlDatesAndCrawlsQuery = null;
	SQLiteStatement  crawlNameQuery = null;
	String webBaseIndexPath = null;
	boolean printErrors = true;
	boolean throwErrors = true;
	int verbose     	= 1;
	
	private static final String URL_DATES_QUERY = "SELECT datesCrawled FROM URLs WHERE url=?";
	private static final int URL_DATES_QUERY_URL_BIND_POS = 1;
	
	private static final String URL_DATES_AND_CRAWL_NAMES_QUERY = "SELECT datesCrawled,crawlIDs FROM URLs WHERE url=?";
	private static final int URL_DATES_AND_CRAWL_NAMES_QUERY_DATES_POS = 0;
	private static final int URL_DATES_AND_CRAWL_NAMES_QUERY_CRAWL_NAMES_POS = 1;
	
	private static final String CRAWL_NAME_QUERY = "SELECT crawlName,startDate,endDate FROM Crawls WHERE shortName=?";
	private static final int CRAWL_NAME_QUERY_BIND_POS = 1;
	private static final int CRAWLS_QUERY_NAME_POS = 0;
	private static final int CRAWLS_QUERY_START_DATE_POS = 1;
	private static final int CRAWLS_QUERY_END_DATE_POS = 2;
	
	// Schema related constants:
	
	private static final int THE_ONLY_COL = 0;
	
	class ResourceSpec {
		String uri;
		GregorianCalendar crawlDate;
		String crawlName;
		
		public ResourceSpec(String theURI, GregorianCalendar theCrawlDate, String theCrawlName) {
			uri = theURI;
			crawlDate = theCrawlDate;
			crawlName = theCrawlName;
		}
		
		public String toString() {
			return "<" + uri + ": " + crawlName + " at " + WBIndex.calendarToString(crawlDate) + ">";
		}
	}
	
	class CrawlSpec {
		String fullName;
		String shortName;
		GregorianCalendar earliestDate;
		GregorianCalendar latestDate;
	}
	
	public WBIndex() throws SQLiteException {
		this(null);
	}
	
	public WBIndex(String newWebBaseIndexPath) throws SQLiteException {
		if (newWebBaseIndexPath == null)
			webBaseIndexPath = "src/test/resources/WBTestIndex";
		else
			webBaseIndexPath = newWebBaseIndexPath;
		try {
			open();
			urlDatesQuery = indexDB.prepare(URL_DATES_QUERY);
			urlDatesAndCrawlsQuery = indexDB.prepare(URL_DATES_AND_CRAWL_NAMES_QUERY);
			crawlNameQuery = indexDB.prepare(CRAWL_NAME_QUERY);
		} catch (SQLiteException e) {
			handleSQLiteException(e);
		}
	}
	
	public void open() throws SQLiteException {
		indexDB = new SQLiteConnection(new File(webBaseIndexPath));
		indexDB.open(false);
	}
	
	public void close() {
		urlDatesQuery.dispose();
		urlDatesAndCrawlsQuery.dispose();
		crawlNameQuery.dispose();
		indexDB.dispose();
		indexDB = null;
	}
	
	public boolean isOpen() {
		return (indexDB != null) && (!indexDB.isDisposed()); 
	}
	
	public ResourceSpec getClosestURLCrawl(String uri, GregorianCalendar referenceDate) throws SQLiteException, DataFormatException {
		
		
		// Get map like, e.g. 
		// "uri":<givenURI>, "datesCrawled":""2012-04-23 23:45:02; 2011-10-02 15:23:40", "crawlIDs":0;1
		// where each 'datesCrawled' entry corresponds to one 'crawlID' by position:
		Map<String,String> crawlDatesAndCrawlIDs = getURLTableCrawlDatesAndCrawlIDs(uri);
		if (crawlDatesAndCrawlIDs == null)
			return null;
		
		// Grab the crawl dates of the uri:
		String datesStr = crawlDatesAndCrawlIDs.get("datesCrawled");
		if (datesStr == null || datesStr.length() == 0)
			return null;
		
		// Turn the string of date/times into a list of Calendar instances:
		ArrayList<GregorianCalendar> dates = parseWBIndexDateChain(datesStr);
		
		// Find date least distant from given reference date: 
		long referenceInMsecs = referenceDate.getTimeInMillis();
		long leastDistance = Math.abs(referenceInMsecs - dates.get(0).getTimeInMillis());
		GregorianCalendar closestTime = dates.get(0); 
		int closesCrawlShortNameIndex = 0;
		
		// Keep track of crawlIDs index while we go through list
		// of Calendar date/times to find the one with minimum distance:
		int  currentCrawlIDIndex  = 0;

		for (GregorianCalendar date : dates.subList(1, dates.size())) {
			long msecsSinceEpoch = date.getTimeInMillis();
			long distance = Math.abs(referenceInMsecs - msecsSinceEpoch);
			if (distance < leastDistance) {
				leastDistance = distance;
				closesCrawlShortNameIndex = currentCrawlIDIndex + 1;
			}
			currentCrawlIDIndex += 1;
		}
		
		// Get the crawl short-names, e.g. "0;1":
		String crawlShortNamesStr = crawlDatesAndCrawlIDs.get("crawlIDs");
		if (crawlShortNamesStr == null) {
			throw new DataFormatException("Crawl short-names field in WebBase index is null. URI: " +
										  uri + "; datesCrawled: " + datesStr);
		}
		String[] crawlShortNamesArray = crawlShortNamesStr.split(";");
		if (crawlShortNamesArray.length < currentCrawlIDIndex) {
			throw new DataFormatException("Fewer crawl short-names than crawl dates. URI: " +
										  uri + "; datesCrawled: " + datesStr + 
										  "; crawlIDs: " + crawlShortNamesStr);
		}
		String crawlShortName = crawlShortNamesArray[closesCrawlShortNameIndex];
		CrawlSpec crawlLookupFromShortName = getCrawlNameFromShortName(crawlShortName);
		String crawlFullName  = crawlLookupFromShortName.fullName;
		
		ResourceSpec result = new ResourceSpec(uri, closestTime, crawlFullName);
		
		return result;
	}

	public ArrayList<GregorianCalendar> getAllURLCrawlDates(String uri) throws SQLiteException {		
		try {
			if (urlDatesQuery.isDisposed())
				urlDatesQuery = indexDB.prepare(URL_DATES_QUERY);
			urlDatesQuery.bind(URL_DATES_QUERY_URL_BIND_POS, uri);
			while (urlDatesQuery.step()) {
				// Query returns one column: the crawl dates, separated by semicolons:
				// as in "2012-04-23 23:45:02; 2011-10-02 15:23:40"
				String datesStr = urlDatesQuery.columnString(THE_ONLY_COL);
				return parseWBIndexDateChain(datesStr);
			}
		} catch (SQLiteException e) {
			handleSQLiteException(e);
		} finally {
			urlDatesQuery.dispose();
		}
		return null;
	}

	private Map<String,String> getURLTableCrawlDatesAndCrawlIDs (String uri) throws SQLiteException {
		Map<String,String> result = new HashMap<String,String>();
		result.put("uri", uri);
		try {
			if (urlDatesAndCrawlsQuery.isDisposed())
				urlDatesAndCrawlsQuery = indexDB.prepare(URL_DATES_AND_CRAWL_NAMES_QUERY);
			urlDatesAndCrawlsQuery.bind(URL_DATES_QUERY_URL_BIND_POS, uri);
			while (urlDatesAndCrawlsQuery.step()) {
				// Query returns two columns: the crawl dates, and a list of 
				// crawl short-names.
				// as in ["2012-04-23 23:45:02; 2011-10-02 15:23:40", "0;1"]
				result.put("datesCrawled", urlDatesAndCrawlsQuery.columnString(URL_DATES_AND_CRAWL_NAMES_QUERY_DATES_POS));
				result.put("crawlIDs", urlDatesAndCrawlsQuery.columnString(URL_DATES_AND_CRAWL_NAMES_QUERY_CRAWL_NAMES_POS));
			}
		} catch (SQLiteException e) {
			handleSQLiteException(e);
		} finally {
			urlDatesAndCrawlsQuery.dispose();
		}
		
		return result;
	}

	public CrawlSpec getCrawlNameFromShortName(String shortName) throws SQLiteException, DataFormatException {
		
		CrawlSpec result = new CrawlSpec();
		result.shortName = shortName;
		
		try {
			if (crawlNameQuery.isDisposed())
				crawlNameQuery = indexDB.prepare(CRAWL_NAME_QUERY);
			crawlNameQuery.bind(CRAWL_NAME_QUERY_BIND_POS, shortName);
			while (crawlNameQuery.step()) {
				// Query returns three columns: [<fullCrawlName>, <earliestDate>, <latestDate>]
				result.fullName     = crawlNameQuery.columnString(CRAWLS_QUERY_NAME_POS);
				String earliestDate = crawlNameQuery.columnString(CRAWLS_QUERY_START_DATE_POS);
				String latestDate   = crawlNameQuery.columnString(CRAWLS_QUERY_END_DATE_POS);
				
				if (result.fullName == null || result.fullName.length() == 0)
					throw new DataFormatException("Full crawl name is null or empty in table URLs for crawlID " + shortName);
				
				if (earliestDate != null && earliestDate.length() > 0) {
					GregorianCalendar date = parseWBIndexDateTime(earliestDate);
					if (date != null)
						result.earliestDate = date;
				}
				
				if (latestDate != null && latestDate.length() > 0) {
					GregorianCalendar  date = parseWBIndexDateTime(latestDate);
					if (date != null)
						result.latestDate = date;
				}
			}
		} catch (SQLiteException e) {
			handleSQLiteException(e);
		} finally {
			crawlNameQuery.dispose();
		}
		
		return result;
	}
	
	
	/**
	 * Given a string with a date and time as stored in the WB index,
	 * return a corresponding GregorianCalendar instance.
	 * Example input:  "2012-04-23 23:45:02".
	 * 
	 * @param dateTime: String of space-separated date and time. 
	 * @return GregorianCalendar instance wrapping the given date/time.
	 */
	private GregorianCalendar parseWBIndexDateTime(String dateTimeStr) {
			// Separate date from time:
			String[] dateTime    = dateTimeStr.split(" ");
			// Get e.g. [2012,04,23]:
			String[] dateArray   = dateTime[0].split("-");
			// Get e.g. [23,45,02]:
			String[] timeArray   =  dateTime[1].split(":");
			// The 'minus 1' is because calendar seems to have month
			// 0-based, yet everything else is 1-based:
			GregorianCalendar cal = new GregorianCalendar (Integer.parseInt(dateArray[0]), 
														   Integer.parseInt(dateArray[1]) - 1, 
														   Integer.parseInt(dateArray[2]),
														   Integer.parseInt(timeArray[0]),
														   Integer.parseInt(timeArray[1]),
														   Integer.parseInt(timeArray[2]));
			return cal;
	}
	
	/**
	 * Given a string-based list of date/time pairs from the WebBase index,
	 * return an ArrayList of corresponding GregorianCalendor instances.
	 * Example input: "2012-04-23 23:45:02; 2011-10-02 15:23:40".
	 * @param dateChainStr: semi-colon-separated list of "<date> <time>". Date in 
	 * 		the form yyyy-MM-dd hh:mm:ss.
	 * @return ArrayList of GregorianCalendar instances, each corresponding to
	 * one of the passed-in date/time string segments. If input is an empty string,
	 * and empty ArrayList is returned.
	 */
	private ArrayList<GregorianCalendar> parseWBIndexDateChain(String dateChainStr) {
		
		ArrayList<GregorianCalendar> result = new ArrayList<GregorianCalendar>(); 
		String[] dates  = dateChainStr.split(";");
		for (String oneDate : dates) {
			result.add(parseWBIndexDateTime(oneDate));
		}
		return result;
	}

	
	public static String calendarToString(GregorianCalendar cal) {
		SimpleDateFormat dateFormat = new SimpleDateFormat();
		dateFormat.applyLocalizedPattern("yyyy-MM-dd HH:mm:ss");
		String dateTimeStr = dateFormat.format(cal.getTime());
		return dateTimeStr;
	}
	
	public List<String[]>poseRawQuery(String sqlStr, int numColsExpected) throws SQLiteException {
		SQLiteStatement st = null;
		String[] row;
		ArrayList<String[]> result = new ArrayList<String[]>();
		
		try {
			st = indexDB.prepare(sqlStr);
		} catch (SQLiteException e) {
			handleSQLiteException(e);
		}
		while (st.step()) {
			row = new String[numColsExpected];
			for (int i=0; i<numColsExpected; i++) {
				row[i] = st.columnString(i);
			}
			result.add(row);
			if (verbose > 0) {
				Iterator<String[]> it = (Iterator<String[]>) result.iterator();
				System.out.print("[");
				while (it.hasNext()) {
					for (String col : it.next()) {
						System.out.println(col);
					}
				}
				System.out.println("]");
			}
		}
		return result;
	}

	@SuppressWarnings("unused")
	private void log(Object obj) {
		if (verbose > 0) {
			System.out.println(obj.toString());
		}
	}
	
	private void handleSQLiteException(SQLiteException e) throws SQLiteException {
		int errCode = e.getErrorCode();
		String errMsg = e.getMessage();
		String finalErrMsg = "WebBase index err: '" + errMsg + "'";
		if (printErrors)
			System.out.println(finalErrMsg);
		if (throwErrors)
			throw new SQLiteException(errCode, finalErrMsg);
	}
	
	public static void main(String[] args) throws SQLiteException, DataFormatException {
		WBIndex index = new WBIndex();
//		//index.poseRawQuery("SELECT time FROM Crawl_state_05_2012 where time = '2012-05-07 06.:10:41'", 1);
//		index.poseRawQuery("SELECT time FROM Crawl_state_05_2012 where time > '2012-05-07 06:10:41'", 1);
//		ArrayList<GregorianCalendar> crawlDates = index.getAllURLCrawlDates("http://agr.wa.gov/robots.txt");
//		//ArrayList<GregorianCalendar> crawlDates = index.getAllURLCrawlDates("http://foo/bar");
//		if (crawlDates.size() == 0) {
//			System.out.println("Result empty.");
//			System.exit(0);
//		} else {
//			for (GregorianCalendar dateTime : crawlDates) {
//				if (dateTime == null)
//					System.out.println("null");
//				else
//					System.out.println(WBIndex.calendarToString(dateTime));
//			}
//		}
//		// Test getURLTableCrawlDatesAndCrawlIDs:
//		Map<String,String> uriDatesAndCrawlIDs = index.getURLTableCrawlDatesAndCrawlIDs("http://agr.wa.gov/robots.txt");
//		if (uriDatesAndCrawlIDs == null) {
//			System.out.println("uriDatesAndCrawlIDs emtpy for " + "http://agr.wa.gov/robots.txt");
//		} else {
//			System.out.println("URI: " + uriDatesAndCrawlIDs.get("uri") +
//							   "; Dates: " + uriDatesAndCrawlIDs.get("datesCrawled") +
//							   "; Crawls: " + uriDatesAndCrawlIDs.get("crawlIDs"));
//		}
//		
//		// Test getCrawlNameFromShortName():
//		// Should be: Name is HurricaneCoverage20051004-text; Earliest date is 2005-10-04 03:00:00; Latest date is 2005-10-04 23:05:00
//		CrawlSpec crawlLookupFromShortName = index.getCrawlNameFromShortName("0");
//		if (crawlLookupFromShortName == null)
//			System.out.println("crawlLookupFromShortName is null.");
//		else
//			System.out.println("Crawl lookup of '0': Name is " + crawlLookupFromShortName.fullName +
//								"; Earliest date is " +  WBIndex.calendarToString(crawlLookupFromShortName.earliestDate) +
//								"; Latest date is " + WBIndex.calendarToString(crawlLookupFromShortName.latestDate));
//
		// Test getting closest crawl date and crawl name for given URI and reference date:
		// Ref date equal to first date in list:
		
		String uri = "http://agr.wa.gov/robots.txt";
		GregorianCalendar refDate = index.parseWBIndexDateTime("2012-05-07 02:51:56");
		ResourceSpec closestMatch = index.getClosestURLCrawl(uri, refDate);
		if (closestMatch == null)
			System.out.println("closestMatch is null.");
		else
			System.out.println("Closest match: " + closestMatch.toString());
		
		// Ref date equal to last date in list:
		refDate = index.parseWBIndexDateTime("2011-11-13 14:51:56");
		closestMatch = index.getClosestURLCrawl(uri, refDate);
		if (closestMatch == null)
			System.out.println("closestMatch is null.");
		else
			System.out.println("Closest match: " + closestMatch.toString());
		
		// Ref date less than first date in list:
		
		// Ref date greater than last date in list:
		
		// URI not found:
		

		
		index.close();
	}
}
