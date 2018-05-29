package cn.com.pander.utils;

/**
 * 校验工具类
 * @author fly
 *
 */
public class ValidUtils {
	
	/**
	 * 校验数据中的指定字段，是否在指定范围内
	 */
	public static boolean between(String data, String dataField, 
			String parameter, String startParamField, String endParamField) {
		String startParamFieldStr = StringUtils.getFieldFromConcatString(
				parameter, "\\|", startParamField);
		String endParamFieldStr = StringUtils.getFieldFromConcatString(
				parameter, "\\|", endParamField); 
		if(startParamFieldStr == null || endParamFieldStr == null) {
			return true;
		}
		
		int startParamFieldValue = Integer.valueOf(startParamFieldStr);
		int endParamFieldValue = Integer.valueOf(endParamFieldStr);
		
		String dataFieldStr = StringUtils.getFieldFromConcatString(
				data, "\\|", dataField);
		if(dataFieldStr != null) {
			int dataFieldValue = Integer.valueOf(dataFieldStr);
			if(dataFieldValue >= startParamFieldValue &&
					dataFieldValue <= endParamFieldValue) {
				return true;
			} else {
				return false;
			}
		}
		
		return false;
	}
	
	/**
	 * 校验数据中的指定字段，是否有值与参数字段的值相同
	 * @param data
	 * @param dataField
	 * @param parameter
	 * @param paramField
	 * @return
	 */
	public static boolean in(String data, String dataField, 
			String parameter, String paramField) {
		String paramFieldValue = StringUtils.getFieldFromConcatString(
				parameter, "\\|", paramField);
		if(paramFieldValue == null) {
			return true;
		}
		String[] paramFieldValueSplited = paramFieldValue.split(",");  
		
		String dataFieldValue = StringUtils.getFieldFromConcatString(
				data, "\\|", dataField);
		if(dataFieldValue != null) {
			String[] dataFieldValueSplited = dataFieldValue.split(",");
			
			for(String singleDataFieldValue : dataFieldValueSplited) {
				for(String singleParamFieldValue : paramFieldValueSplited) {
					if(singleDataFieldValue.equals(singleParamFieldValue)) {
						return true;
					}
				}
			}
 		}
		return false;
	}
	
	/**
	 * 校验数据中的指定字段，是否相同
	 * @param data
	 * @param dataField
	 * @param parameter
	 * @param paramField
	 * @return
	 */
	public static boolean equal(String data, String dataField, 
			String parameter, String paramField) {  
		String paramFieldValue = StringUtils.getFieldFromConcatString(
				parameter, "\\|", paramField);
		if(paramFieldValue == null) {
			return true;
		}
		
		String dataFieldValue = StringUtils.getFieldFromConcatString(
				data, "\\|", dataField);
		if(dataFieldValue != null) {
			if(dataFieldValue.equals(paramFieldValue)) {
				return true;
			}
 		}
		
		return false;
	}

}
