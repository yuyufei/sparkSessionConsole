package cn.com.pander.spark;

import org.apache.spark.util.AccumulatorV2;

import cn.com.pander.constant.Constants;
import cn.com.pander.utils.StringUtils;

/**
 * session聚合统计
 * @author fly
 *
 */
public class SessionAggrStatAccumulator  extends AccumulatorV2<String, String>{

	private static final long serialVersionUID = 1L;

	/**
	 * 用于数据的初始化
	 */
	public String zero(String arg0) {
		return Constants.SESSION_COUNT + "=0|"
				+ Constants.TIME_PERIOD_1s_3s + "=0|"
				+ Constants.TIME_PERIOD_4s_6s + "=0|"
				+ Constants.TIME_PERIOD_7s_9s + "=0|"
				+ Constants.TIME_PERIOD_10s_30s + "=0|"
				+ Constants.TIME_PERIOD_30s_60s + "=0|"
				+ Constants.TIME_PERIOD_1m_3m + "=0|"
				+ Constants.TIME_PERIOD_3m_10m + "=0|"
				+ Constants.TIME_PERIOD_10m_30m + "=0|"
				+ Constants.TIME_PERIOD_30m + "=0|"
				+ Constants.STEP_PERIOD_1_3 + "=0|"
				+ Constants.STEP_PERIOD_4_6 + "=0|"
				+ Constants.STEP_PERIOD_7_9 + "=0|"
				+ Constants.STEP_PERIOD_10_30 + "=0|"
				+ Constants.STEP_PERIOD_30_60 + "=0|"
				+ Constants.STEP_PERIOD_60 + "=0";
	}

	/**
	 * session统计计算逻辑
	 * @return
	 */
	private String add(String v1,String v2) 
	{
		if(StringUtils.isEmpty(v1))
			return v2;
		String oldValue=StringUtils.getFieldFromConcatString(v1, "\\|", v2);
		if(oldValue != null) 
		{
			int newValue=Integer.valueOf(oldValue)+1;
			return StringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue));
		}
		return v1;
	}


	@Override
	public void add(String arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public AccumulatorV2<String, String> copy() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isZero() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void merge(AccumulatorV2<String, String> arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String value() {
		// TODO Auto-generated method stub
		return null;
	}


}
