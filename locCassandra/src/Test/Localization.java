package Test;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.poi.*;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class Localization {
		public static boolean isRowEmpty(Row row) {
		for (int c = row.getFirstCellNum(); c < row.getLastCellNum(); c++) {
			Cell cell = row.getCell(c);
			if (cell != null && cell.getCellType() != Cell.CELL_TYPE_BLANK)
				return false;
		}
		return true;
	}
		
	public static void main(String[] args) throws IOException {
		// readExcel();
		Cluster cluster;
		Session session;
		// create keyspace
		// Connect to the cluster and keyspace "loc"
		cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		session = cluster.connect("loc");

		// create keyspace -create manually
		session.execute(
				"CREATE KEYSPACE IF NOT EXISTS loc WITH replication = {'class': 'SimpleStrategy','replication_factor': '3'}");

		// create table
		
		session.execute(
				"CREATE TABLE IF NOT EXISTS phrases (apikey text, domain int, english text, hindi text, telugu text, tamil text, kannada text, malayalam text, marathi text, gujarati text, punjabi text, bengali text, assamese text, oriya text, PRIMARY KEY (english,domain));");
		session.execute("CREATE INDEX IF NOT EXISTS on phrases(domain)");
		session.execute("CREATE INDEX IF NOT EXISTS on phrases(apikey)");
		

		// Reading excel file
		String excelFilePath = "datafull.xlsx";
		FileInputStream locFile = new FileInputStream(new File(excelFilePath));
		File file = new File(excelFilePath);
		String totalLines = null;
		// byte[] bytes = file.getBytes();

		InputStreamReader suzy = new InputStreamReader(locFile, "UTF-8");
		XSSFWorkbook workbook = new XSSFWorkbook(locFile);

		// Get first sheet from the workbook
		XSSFSheet sheet = workbook.getSheetAt(0);

		// Get iterator to all the rows in current sheet
		Iterator<Row> iterator = sheet.iterator();

		ArrayList<Object> userValues = new ArrayList<Object>();
		ArrayList<String> userLanguage = new ArrayList<String>();

		int langCount = 0;
		while (iterator.hasNext()) {
			Row nextRow = iterator.next();
			if (isRowEmpty(nextRow)) {
				break;
			} else {
				Iterator<Cell> cellIterator = nextRow.cellIterator();
				langCount++;

				while (cellIterator.hasNext()) {
					Cell cell = cellIterator.next();
					switch (cell.getCellType()) {
					case Cell.CELL_TYPE_BLANK:
						break;
					case Cell.CELL_TYPE_STRING:
						if (langCount < 2) {
							userLanguage.add(cell.getStringCellValue().toLowerCase());
							
						} else {
							userValues.add(cell.getStringCellValue());
							
						}
						break;
					case Cell.CELL_TYPE_NUMERIC:
						Double num = cell.getNumericCellValue();
						userValues.add(num.intValue());
						break;
					}
				}
				String[] headers = userLanguage.toArray(new String[userLanguage.size()]);
				if(langCount >1) {
					Statement localizeQuery = QueryBuilder.insertInto("phrases").values(headers, userValues.toArray());
					//System.out.println(localizeQuery);
					session.execute(localizeQuery);
					userValues.removeAll(userValues);
				}
			}
		}

		ResultSet results = session.execute("SELECT * FROM phrases");
		for (com.datastax.driver.core.Row row3 : results) {
			System.out.format("%s %d %s %s %s\n", row3.getString("english"), row3.getInt("domain"),
					row3.getString("hindi"), row3.getString("telugu"), row3.getString("tamil"),
					row3.getString("kannada"));
		}
		
		String apiQuery = "SELECT english,apikey FROM phrases WHERE apikey = '3612ca85654b6de85942c929abe7193b4713' ";
		System.out.println(apiQuery);
		results = session.execute(apiQuery);
		for (com.datastax.driver.core.Row row2 : results) {
			System.out.format("%s %s \n", row2.getString("english"), row2.getString("apikey"));
		}
		
		String basicQuery = "SELECT english,hindi FROM phrases WHERE english='Cancel Ticket' AND domain = 2 "; 
		
		results = session.execute(basicQuery);
		for (com.datastax.driver.core.Row row : results) {
			System.out.format("%s %s \n", row.getString("english"), row.getString("hindi"));
		}
		cluster.close();
	}
}
