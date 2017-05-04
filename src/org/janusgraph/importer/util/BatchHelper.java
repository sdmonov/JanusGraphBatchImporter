package org.janusgraph.importer.util;

import java.io.File;
import java.io.FileReader;
import java.io.LineNumberReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class BatchHelper {
	public static final SimpleDateFormat DATE_PARSER_1 = new SimpleDateFormat("dd-MMM-yyyy");
	public static final SimpleDateFormat DATE_PARSER_2 = new SimpleDateFormat("yyyy-mm-dd");
	public static final SimpleDateFormat DATE_PARSER_3 = new SimpleDateFormat("dd/mm/yy");
	public static final SimpleDateFormat DATE_PARSER_4 = new SimpleDateFormat("dd.mm.yyyy");
	public static final SimpleDateFormat DATE_PARSER_5 = new SimpleDateFormat("dd/mm/yyyy");

	public static long countLines(String filePath) throws Exception {
		LineNumberReader lnr = new LineNumberReader(new FileReader(new File(filePath)));
		try {
			lnr.skip(Long.MAX_VALUE);
			return lnr.getLineNumber() + 1;
		} finally {
			lnr.close();
		}
	}
	
	public static Date convertDate(String inputDate) throws ParseException {
		// Detect the date format and convert it
		if (inputDate.matches("[0-9]{2}-[A-Za-z]{3}-[0-9]{4}")) {
			// Use dd-MMM-yyyy format
			return DATE_PARSER_1.parse(inputDate);
		} else if (inputDate.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}")) {
			// Use yyyy-mm-dd format
			return DATE_PARSER_2.parse(inputDate);
		} else if (inputDate.matches("[0-9]{2}/[0-9]{2}/[0-9]{2}")) {
			// Use dd/mm/yy format
			return DATE_PARSER_3.parse(inputDate);
		} else if (inputDate.matches("[0-9]+\\.[0-9]{2}\\.[0-9]{4}")) {
			// dd.mm.yyyy
			return DATE_PARSER_4.parse(inputDate);
		} else {
			// Default use dd/mm/yyyy
			return DATE_PARSER_5.parse(inputDate);
		}
	}

}
