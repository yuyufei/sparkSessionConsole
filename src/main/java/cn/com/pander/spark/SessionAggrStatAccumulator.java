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
	
	private  String initialStr=Constants.SESSION_COUNT + "=0|"
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

	
	private String[] initialArray= {Constants.SESSION_COUNT ,Constants.TIME_PERIOD_1s_3s ,Constants.TIME_PERIOD_4s_6s , Constants.TIME_PERIOD_7s_9s,
			 									 Constants.TIME_PERIOD_10s_30s,  Constants.TIME_PERIOD_30s_60s,Constants.TIME_PERIOD_1m_3m ,Constants.TIME_PERIOD_3m_10m,
			 									 Constants.TIME_PERIOD_10m_30m, Constants.TIME_PERIOD_30m, Constants.STEP_PERIOD_1_3 ,Constants.STEP_PERIOD_4_6 ,
			 									 Constants.STEP_PERIOD_7_9 ,Constants.STEP_PERIOD_10_30 ,Constants.STEP_PERIOD_30_60 ,Constants.STEP_PERIOD_60 };

  /**
   * session统计累加逻辑
   */
	@Override
	public void add(String v2) {
		String v1=initialStr;
		if(StringUtils.isEmpty(v1))
			return;
		String oldValue=StringUtils.getFieldFromConcatString(v1, "\\|", v2);
		if(oldValue != null) 
		{
			int newValue=Integer.valueOf(oldValue)+1;
			initialStr=StringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue));
		}
	}

	/**
	 * 拷贝一个新的AccumulatorV2
	 */
	@Override
	public AccumulatorV2<String, String> copy() {
		SessionAggrStatAccumulator accumulator=new SessionAggrStatAccumulator();
		accumulator.initialStr=this.initialStr;
		return accumulator;
	}

	/**
	 * 如果累加器为0值，则返回
	 */
	@Override
	public boolean isZero() {
		return true;
	}

	/**
	 * 各个task的累加器的合并
	 */
	@Override
	public void merge(AccumulatorV2<String, String> accumulator) {
		if(accumulator == null)
			return;
		else 
		{
			SessionAggrStatAccumulator obj=new SessionAggrStatAccumulator();
			if(accumulator.getClass().isInstance(obj)) 
			{
				SessionAggrStatAccumulator ss=(SessionAggrStatAccumulator) accumulator;
				//按照数组定义的字段进行各个累加器的值进行累加聚合
				for(int i=0;i<initialArray.length;i++) 
				{
					String v1=initialStr;
					String v2=initialArray[i];
					String v3=ss.initialStr;
					if(v2!=null && !"".equals(v2)) 
					{
						String oldValue=StringUtils.getFieldFromConcatString(v3, "\\|", v2);
						String originalVal=StringUtils.getFieldFromConcatString(v1, "\\|", v2);
						if(oldValue != null) 
						{
							int newValue=Integer.valueOf(oldValue)+Integer.valueOf(originalVal);
							initialStr=StringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue));
						}
					}
				}
			}
		}
	}

	/**
	 * 重置
	 */
	@Override
	public void reset() {
		initialStr=Constants.SESSION_COUNT + "=0|"
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

	@Override
	public String value() {
		return initialStr;
	}


}
