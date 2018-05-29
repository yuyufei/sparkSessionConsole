package cn.com.pander.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;


public class DateUtils {
	
	public static final SimpleDateFormat TIME_FORMAT = 
			new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public static final SimpleDateFormat DATE_FORMAT = 
			new SimpleDateFormat("yyyy-MM-dd");
	public static final SimpleDateFormat DATEKEY_FORMAT=
			new SimpleDateFormat("yyyyMMdd");
	
	
	/**
	 * 判断一个时间是否在另一个时间之前
	 * @return
	 */
	public static boolean before(String time1,String time2) 
	{
		try {
			Date date1=TIME_FORMAT.parse(time1);
			Date date2=TIME_FORMAT.parse(time2);
			if(date1.before(date2))
				return true;
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		return false;
	}
	
	/**
	 * 判断一个时间在另一个时间之后
	 */
	public static boolean after(String time1,String time2) 
	{
		try {
			Date date1=TIME_FORMAT.parse(time1);
			Date date2=TIME_FORMAT.parse(time2);
			if(date1.after(date2))
				return true;
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		return false;
	}
	
	/**
	 * 计算时间差值
	 * @return
	 */
	public static int minus(String time1,String time2) 
	{
		try {
			Date date1=TIME_FORMAT.parse(time1);
			Date date2=TIME_FORMAT.parse(time2);
			long millisecond=date1.getTime()-date2.getTime();
			return Integer.valueOf(String.valueOf(millisecond/1000));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		return 0;
	}

	/**
	 * 获取年月日和小时
	 * @return
	 */
	public static String getDateHour(String datetime) 
	{
		String date=datetime.split(" ")[0];
		String hourMinuteSecond=datetime.split(" ")[1];
		String hour = hourMinuteSecond.split(":")[0];
		return date+"_"+hour;
	}
	
	
	/**
	 * 获取当天日期
	 */
	public static String getTodayDate() 
	{
		return DATE_FORMAT.format(new Date());
	}
	
	/**
	 * 获取昨天的日期
	 * @return
	 */
	public static String getYesterdayDate() 
	{
		Calendar cal=Calendar.getInstance();
		cal.setTime(new Date());
		cal.add(Calendar.DAY_OF_YEAR, -1);
		Date date=cal.getTime();
		return DATE_FORMAT.format(date);
	}
	
	/**
	 * 格式化日期:yyyy-MM-dd
	 */
	public static String foramtDate(Date date) 
	{
		return DATE_FORMAT.format(date);
	}
	
	/**
	 * 格式化日期 :yyyy-MM-dd HH:mm:ss
	 */
	public static String formatTime(Date date) 
	{
		return TIME_FORMAT.format(date);
	}
	
	/**
	 * 解析时间字符串
	 * @throws ParseException 
	 */
	public static Date parseTime(String time) throws ParseException 
	{
		return TIME_FORMAT.parse(time);
	}
	
	/**
	 * 格式化日期
	 */
	public static String formatDateKey(Date date) 
	{
		return DATEKEY_FORMAT.format(date);
	}
	
	/**
	 * 格式化日期
	 * @throws ParseException 
	 */
	public static Date parseDateKey(String datekey) throws ParseException 
	{
		return DATEKEY_FORMAT.parse(datekey);
	}
	
	/**
	 *格式化时间，保留到分钟级别 
	 * @param date
	 * @return
	 */
	public static String formatTimeMinute(Date date) 
	{
		SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMddHHmm");
		return sdf.format(date);
	}
	
}
