package edu.stanford.arcspread;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

public class WBIndex {
	
	SQLiteConnection indexDB = null;
	String webBaseIndexPath = null;
	boolean printErrors = true;
	boolean throwErrors = true;
	int verbose     	= 1;
	
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
		} catch (SQLiteException e) {
			handleSQLiteException(e);
		}
	}
	
	public void open() throws SQLiteException {
		indexDB = new SQLiteConnection(new File(webBaseIndexPath));
		indexDB.open(false);
	}
	
	public void close() {
		indexDB.dispose();
		indexDB = null;
	}
	
	public boolean isOpen() {
		return (indexDB != null) && (!indexDB.isDisposed()); 
	}
	
	
	public ArrayList<java.sql.Time> allCrawlDates(String uri) {
		return null;
	}
	
	
	public ArrayList<T> poseRawQuery(String str, ArrayList<T> result) {
		
	}
	
	public ArrayList<String[]>poseRawQuery(String sqlStr, int numColsExpected) throws SQLiteException {
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

	private void log(Object obj) {
		if (verbose > 0) {
			System.out.println(obj.toString());
		}
	}
	
	private void handleSQLiteException(SQLiteException e) throws SQLiteException {
		int errCode = e.getErrorCode();
		String errMsg = e.getMessage();
		String recentSqliteErrMsg = "";
		try {
			recentSqliteErrMsg = indexDB.getErrorMessage();
		} catch (SQLiteException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		String finalErrMsg = "WebBase index err: '" + errMsg + "'";
		if (printErrors)
			System.out.println(finalErrMsg);
		if (throwErrors)
			throw new SQLiteException(errCode, finalErrMsg);
	}
	
	public static void main(String[] args) throws SQLiteException {
		WBIndex index = new WBIndex();
		//index.poseRawQuery("SELECT time FROM Crawl_state_05_2012 where time = '2012-05-07 06:10:41'", 1);
		index.poseRawQuery("SELECT time FROM Crawl_state_05_2012 where time > '2012-05-07 06:10:41'", 1);
		index.close();
	}
}
