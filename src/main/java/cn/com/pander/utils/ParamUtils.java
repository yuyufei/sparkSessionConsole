package cn.com.pander.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import cn.com.pander.conf.ConfigurationManager;
import cn.com.pander.constant.Constants;

/**
 * 参数工具类
 * @author fly
 *
 */
public class ParamUtils {
	
	/**
	 * 从任务行信息获取任务id
	 * @param args
	 * @param taskType
	 * @return
	 */
	public static Long getTaskIdFromArgs(String[] args, String taskType) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		
		if(local) {
			return ConfigurationManager.getLong(taskType);  
		} else {
			try {
				if(args != null && args.length > 0) {
					return Long.valueOf(args[0]);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		return null;
	}
	
	/**
	 * 从json对象中提取参数
	 * @param jsonObject
	 * @param field
	 * @return
	 */
	public static String getParam(JSONObject jsonObject, String field) {
		JSONArray jsonArray = jsonObject.getJSONArray(field);
		if(jsonArray != null && jsonArray.size() > 0) {
			return jsonArray.getString(0);
		}
		return null;
	}

}
