package cn.com.pander.spark;

import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import cn.com.pander.constant.Constants;
import cn.com.pander.test.MockData;
import cn.com.pander.utils.StringUtils;
import scala.Tuple2;

public class UserVisitSessionConsole {
	public static void main(String[] args) {
		SparkConf conf=new SparkConf()
				.setMaster("local[4]").setAppName("userVisitSessionConsole");
		JavaSparkContext sc=new JavaSparkContext(conf);
		SQLContext sqlContext=SparkSession.builder().config(conf).getOrCreate().sqlContext();
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
		sessionid2AggrInfoRDD.collect();
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
					Long clickCategoryId=1l;
					if(StringUtils.isNotEmpty(searchKeyword)) {
						if(!searchKeyWords.toString().contains(searchKeyword)) {
							searchKeyWords.append(searchKeyword + ",");  
						}
					}
					if(clickCategoryId != null) {
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

}
