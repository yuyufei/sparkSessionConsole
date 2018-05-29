package cn.com.pander.utils;

/**
 * 字符串工具类
 * @author fly
 *
 */
public class StringUtils {
	
	/**
	 * 判断字符串是否为空
	 * @return
	 */
	public static boolean isEmpty(String str) 
	{
		return str == null || "".equals(str);
	}
	
	/**
	 * 判断字符串不为空
	 * @return
	 */
	public static boolean isNotEmpty(String str) 
	{
		return str != null && !"".equals(str);
	}
	
	/**
	 * 截断字符串两端的逗号
	 * @return
	 */
	public static String trimComma(String str) 
	{
		if(str.startsWith(",")) 
			str=str.substring(1);
		if(str.endsWith(",")) 
			str=str.substring(0, str.length()-1);
		return str;
	}
	
	/**
	 * 补全两位数字
	 * @return
	 */
	public static String fulfuill(String str) 
	{
		if(str.length() == 2)
			return str;
		else
			return "0"+str;
	}
	
	/**
	 * 从拼接的字符串提取字段
	 * @return
	 */
	public static String getFieldFromConcatString(String str,String delimiter,String field) 
	{
		try {
			String[] fields = str.split(delimiter);
			for(String concatField : fields) {
				// searchKeywords=|clickCategoryIds=1,2,3
				if(concatField.split("=").length == 2) {
					String fieldName = concatField.split("=")[0];
					String fieldValue = concatField.split("=")[1];
					if(fieldName.equals(field)) {
						return fieldValue;
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 为拼接字段设定值
	 * @param str
	 * @param delimiter
	 * @param field
	 * @param newFieldValue
	 * @return
	 */
	public static String setFieldInConcatString(String str, 
			String delimiter, String field, String newFieldValue) {
		String[] fields = str.split(delimiter);
		
		for(int i = 0; i < fields.length; i++) {
			String fieldName = fields[i].split("=")[0];
			if(fieldName.equals(field)) {
				String concatField = fieldName + "=" + newFieldValue;
				fields[i] = concatField;
				break;
			}
		}
		
		StringBuffer buffer = new StringBuffer("");
		for(int i = 0; i < fields.length; i++) {
			buffer.append(fields[i]);
			if(i < fields.length - 1) {
				buffer.append("|");  
			}
		}
		
		return buffer.toString();
	}

}
