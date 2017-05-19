package com.athena.component.input;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;


/**
 * 3300总成-零件-消耗点-系数
 * @date 2015-11-19
 * @author yz
 */
public class ZongccfjgDbDataReader extends TxtInputDBSerivce{
	public  Date date=new Date();
	protected static Logger logger = Logger.getLogger(ZongccfjgDbDataReader.class);	//定义日志方法
	private String datasourceId = "";
	
	public ZongccfjgDbDataReader(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
		datasourceId = dataParserConfig.getWriterConfig().getDatasourceId();
	}
	
	/**
	 * 接口运行前清除in_zongccfjg表
	 */
	@Override
	public void before() {
		try {
			//清表
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutddbh.deletein_zongccfjg");
		}catch (RuntimeException e) {
			logger.error("线程--接口" + interfaceId + "表清除错误！  " + e.getMessage());
		} 
	}



	/**
	 * 行解析之前处理方法
	 *	不读取文件头和文件尾 
	 */
	@Override
	public boolean beforeRecord(String line, String fileName, int lineNum){
		boolean result = true;
		if (line!=null&&line.toString().contains("BEGIN")) {
			result = false;
		}
		if(line!=null&&line.toString().contains("END")){
			result = false;
		}
    	return result;
	}
	
	/**
	 * 接口运行后
	 * 进行新老消耗点的替换
	 * 进行单位换算
	 * 清除历史数据
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void after() {
		try {
			//新老消耗点的替换
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutddbh.updateXiaohd");
		}catch (RuntimeException e) {
			logger.error("线程--接口" + interfaceId + "新老消耗点替换错误！  " + e.getMessage());
		} 
		//查询出数量还有单位
		List<Map<String,Object>> lingjdwList = dataParserConfig.getBaseDao()
		.getSdcDataSource(datasourceId).select("inPutddbh.queryDanwShulzongccfjg");
		for(Map<String,Object> map:lingjdwList){
			try {
				//更新in_zongccfjg表
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutddbh.updatezongccfjg", map);
			}catch (RuntimeException e) {
				logger.error("线程--接口" + interfaceId + "单位换算错误！  " + e.getMessage());
			} 			
		}
		//2016-3-11 yz
//		try {
//			//清除ckx_lingjxhd_s表数据
//			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutddbh.deleteLissj");
//		}catch (RuntimeException e) {
//			logger.error("线程--接口" + interfaceId + "清除表错误！  " + e.getMessage());
//		} 	
		
	}
	
	
}
