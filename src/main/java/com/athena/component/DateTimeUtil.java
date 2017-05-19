package com.athena.component;

import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.time.DateUtils;
import org.apache.log4j.Logger;

public final class DateTimeUtil {
	protected static Logger logger = Logger.getLogger(DateTimeUtil.class); // 定义日志方法

	private static final String[] str1 = new String[] { "yyyy-MM-dd",
			"yyyy-MM-dd HH:mm:ss.SSS", "yyyy-MM-dd HH:mm:ss", "yyyyMMdd",
			"yyyyMMddHHmm", "yyyy-MM-dd HH:mm" };
	private static final String[] str2 = new String[] {
			"yyyy-MM-dd HH:mm:ss.SSS", "yyyy-MM-dd HH:mm:ss", "yyyyMMdd",
			"yyyyMMddHHmm", "yyyy-MM-dd HH:mm", "yyyy-MM-dd" };

	private DateTimeUtil() {

	}

	/** */
	/**
	 * 以格式format返回表示日期时间的字符串
	 * 
	 * @param format
	 * @return
	 */
	public static String getDateTimeStr(String format) {
		Date date = new Date();
		Format formatter = new SimpleDateFormat(format);
		return formatter.format(date);
	}

	/**
	 * 获取当前日期时间
	 * 
	 * @return
	 */
	public static String getAllCurrTime() {
		return getDateTimeStr("yyyy-MM-dd HH:mm:ss");
	}

	public static String getMonth() {
		return getDateTimeStr("yyyyMM");
	}

	/**
	 * yyyy-MM-dd时间格式化
	 * 
	 * @param date
	 * @return String
	 */
	public static String DateStr(String date) {
		String datetime = "";
		if (null != date && !"".equals(date)) {
			String yyyy = date.substring(0, 4);
			String mm = date.substring(4, 6);
			String dd = date.substring(6, 8);
			datetime = yyyy + "-" + mm + "-" + dd;
		}
		return datetime;
	}

	/**
	 * yyyy-MM-dd时间格式化
	 * 
	 * @param DateTime
	 * @return
	 */
	public static String DateFormat(String DateTime) {
		String time = "";
		try {
			if (null != DateTime && !"".equals(DateTime)) {
				String yyyy = DateTime.substring(0, 4);
				String mm = DateTime.substring(4, 6);
				String dd = DateTime.substring(6, 8);
				time = yyyy + "-" + mm + "-" + dd;
			}
		} catch (RuntimeException e) {
			throw new RuntimeException(e.getMessage());
		}
		return time;
	}

	/**
	 * yyyy-mm-dd hh:mm:ss时间格式化
	 * 
	 * @param datestr
	 * @return String
	 */
	public static String SubString(String datestr) {
		String datetime = null;
		try {
			if (null != datestr && !"".equals(datestr)) {
				String yyyy = datestr.substring(0, 4);
				String mm = datestr.substring(4, 6);
				String dd = datestr.substring(6, 8);
				String hh = datestr.substring(8, 10);
				String mi = datestr.substring(10, 12);
				String ss = datestr.substring(12, 14);
				datetime = yyyy + "-" + mm + "-" + dd + " " + hh + ":" + mi
						+ ":" + ss;
			}
		} catch (RuntimeException e) {
			throw new RuntimeException(e.getMessage());
		}
		return datetime;
	}

	/**
	 * yyyy-mm-dd hh:mm时间格式化
	 * 
	 * @param dateStr
	 * @return String
	 */
	public static String DateFormat_Fhtz(String dateStr) {
		String datetime = null;
		try {
			if (null != dateStr && !"".equals(dateStr)) {
				String yyyy = dateStr.substring(0, 4);
				String mm = dateStr.substring(4, 6);
				String dd = dateStr.substring(6, 8);
				String hh = dateStr.substring(8, 10);
				String mi = dateStr.substring(10, 12);
				datetime = yyyy + "-" + mm + "-" + dd + " " + hh + ":" + mi;
			}
		} catch (RuntimeException e) {
			throw new RuntimeException(e.getMessage());
		}
		return datetime;
	}

	/**
	 * 日期转换
	 * 
	 * @param dataSrc
	 * @return Date
	 */
	public static Date StringYMDToDate(String dataSrc) throws ParseException {
		Date endTime = null;
		String[] str = makeDateFormatOrder(dataSrc);
		if (null != dataSrc && !"".equals(dataSrc)) {
			endTime = DateUtils.parseDate(dataSrc.trim(), str);
		}
		return endTime;

	}

	private static String[] makeDateFormatOrder(String dataSrc) {

		String[] str = null;
		Pattern p = Pattern.compile("\\d{4}-\\d{2}-\\d{2}");
		Matcher m = p.matcher(dataSrc);

		if (m.matches()) {
			str = str1;
		} else {
			str = str2;
		}
		return str;
	}

}
