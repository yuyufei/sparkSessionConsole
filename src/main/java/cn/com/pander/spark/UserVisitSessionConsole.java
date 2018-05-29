package cn.com.pander.spark;

import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import com.alibaba.fastjson.JSONObject;

import cn.com.pander.bean.Task;
import cn.com.pander.constant.Constants;
import cn.com.pander.test.MockData;
import cn.com.pander.utils.ParamUtils;
import cn.com.pander.utils.StringUtils;
import cn.com.pander.utils.ValidUtils;
import scala.Tuple2;

public class UserVisitSessionConsole {
	public static void main(String[] args) {
		SparkConf conf=new SparkConf()
				.setMaster("local[4]").setAppName("userVisitSessionConsole");
		JavaSparkContext sc=new JavaSparkContext(conf);
		SQLContext sqlContext=SparkSession.builder().config(conf).getOrCreate().sqlContext();
		Task task =new Task();
		JSONObject taskParam=JSONObject.parseObject(task.getTaskParam());
		MockData.mock(sc, sqlContext);
		String startDate="2018-05-29";
		String endDate="2018-05-29";
		String querySql="select * "
				+ "from user_visit_action "
				+ "where date>='" + startDate + "' "
				+ "and date<='" + endDate + "'";  
		Dataset<Row> ds= sqlContext.sql(querySql);
		System.out.println("---------------------------查询出的数据--------------------------------");
		ds.show();
		JavaRDD<Row> rdd1=ds.javaRDD();
		JavaPairRDD<String, String> sessionid2AggrInfoRDD = 
				aggregateBuSession(sqlContext, rdd1);
		JavaPairRDD<String, String> fileteredSessionid2AggrInfoRDD=
				filterSession(sessionid2AggrInfoRDD,taskParam);
		sc.close();
		
	}
	
	
	/**
	 * 对行为数据按session粒度进行聚合
	 * @return
	 */
	private static JavaPairRDD<String, String> aggregateBuSession(SQLContext sqlContext,JavaRDD<Row> rdd1)
	{
		//转换成rdd对key:sessionid value:row
		JavaPairRDD<String, Row> rdd2=rdd1.mapToPair(
				new PairFunction<Row, String, Row>() {
					private static final long serialVersionUID = 1L;
					@Override
					public Tuple2<String, Row> call(Row t) throws Exception {
						return new Tuple2<String, Row>(t.getString(2), t);
					}
				});
		//按照sessionid进行聚合
		JavaPairRDD<String, Iterable<Row>> rdd3=rdd2.groupByKey();
		//对每个sessionid分组进行聚合，形成long、String的类型
		JavaPairRDD<Long, String> rdd4=rdd3.mapToPair(new PairFunction<Tuple2<String,Iterable<Row>>, Long, String>() 
		{
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> t) throws Exception {
				String sessionid=t._1;
				Iterator<Row> iterator=t._2.iterator();
				StringBuffer searchKeyWords=new StringBuffer();
				StringBuffer clickCategoryIds=new StringBuffer();
				Long userid=null;
				while(iterator.hasNext()) 
				{
					Row row=iterator.next();
					if(userid==null) 
					{
						userid=row.getLong(1);
					}
					String searchKeyword=row.getString(5);
					Long clickCategoryId=row.getLong(6);
					if(StringUtils.isNotEmpty(searchKeyword)) {
						if(!searchKeyWords.toString().contains(searchKeyword)) {
							searchKeyWords.append(searchKeyword + ",");  
						}
					}
					if(clickCategoryId != null && clickCategoryId==1l) {
						if(!clickCategoryIds.toString().contains(
								String.valueOf(clickCategoryId))) {   
							clickCategoryIds.append(clickCategoryId + ",");  
						}
					}
				}
				String searchwords = StringUtils.trimComma(searchKeyWords.toString());
				String clickIds = StringUtils.trimComma(clickCategoryIds.toString());
				String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|"
						+ Constants.FIELD_SEARCH_KEYWORDS + "=" + searchwords + "|"
						+ Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickIds;
				return new Tuple2<Long, String>(userid,partAggrInfo);
			}
		});
		String sql="select * from user_info";
		JavaRDD<Row> userinfoRDD=sqlContext.sql(sql).javaRDD();
		JavaPairRDD<Long, Row> userid2InfoRDD=userinfoRDD.mapToPair(new PairFunction<Row,Long, Row>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<Long, Row> call(Row t) throws Exception {
				return new Tuple2<Long, Row>(t.getLong(0), t);
			}
		});
		JavaPairRDD<Long, Tuple2<String, Row>> userid2FullinfoRDD=
				rdd4.join(userid2InfoRDD);
		JavaPairRDD<String, String> sessionid2FullinfoRDD =
				userid2FullinfoRDD.mapToPair(new PairFunction<Tuple2<Long,Tuple2<String,Row>>, String, String>(
						) {
							private static final long serialVersionUID = 1L;
							@Override
							public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> t) throws Exception {
								String partAggrinfo=t._2._1;
								Row userinfoRow=t._2._2;
								String sessionid = StringUtils.getFieldFromConcatString(
										partAggrinfo, "\\|", Constants.FIELD_SESSION_ID);
								int age = userinfoRow.getInt(3);
								String professional = userinfoRow.getString(4);
								String city = userinfoRow.getString(5);
								String sex = userinfoRow.getString(6);
								
								String fullAggrInfo = partAggrinfo + "|"
										+ Constants.FIELD_AGE + "=" + age + "|"
										+ Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
										+ Constants.FIELD_CITY + "=" + city + "|"
										+ Constants.FIELD_SEX + "=" + sex;
								return new Tuple2<String, String>(sessionid, fullAggrInfo);
							}
				});
		return sessionid2FullinfoRDD;
	}
	
	/**
	 * 过滤session数据
	 */
	private static JavaPairRDD<String, String> filterSession(	JavaPairRDD<String, String> sessionid2AggrInfoRDD, 
			final JSONObject taskParam)
	{
		String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
		String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
		String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
		String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
		String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
		String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
		String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);
		
		String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
				+ (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
				+ (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
				+ (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
				+ (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
				+ (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
				+ (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds: "");
		System.out.println("-------------------------------------------"+_parameter+"----------------------------------------------------");
		if(_parameter.endsWith("\\|")) {
			_parameter = _parameter.substring(0, _parameter.length() - 1);
		}
		
		final String parameter = _parameter;
		//根据筛选参数进行过滤
		JavaPairRDD<String, String> fileteredSessionId2AggrInfoRDD = 
				sessionid2AggrInfoRDD.filter(new Function<Tuple2<String,String>, Boolean>() {
					private static final long serialVersionUID = 1L;
					@Override
					public Boolean call(Tuple2<String, String> tuple) throws Exception {
						String aggrInfo=tuple._2;
						//年龄范围筛选
						if(!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, 
								parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
							return false;
						}
						//职业范围筛选
						if(!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, 
								parameter, Constants.PARAM_PROFESSIONALS)) {
							return false;
						}
						//城市范围过滤
						if(!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, 
								parameter, Constants.PARAM_CITIES)) {
							return false;
						}
						//按照性别过滤
						if(!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX, 
								parameter, Constants.PARAM_SEX)) {
							return false;
						}
						//按照搜索词过滤
						if(!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, 
								parameter, Constants.PARAM_KEYWORDS)) {
							return false;
						}
						//按照点击品类id过滤
						if(!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, 
								parameter, Constants.PARAM_CATEGORY_IDS)) {
							return false;
						}
						return true;
					}
				});
		return fileteredSessionId2AggrInfoRDD;
	}
	
	

}
