package com.athena.component.output;

import org.apache.log4j.Logger;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.db.DBOutputTxtSerivce;


/**
 * 分装线排产计划 3270
 * @date 2015-12-2
 * @author yz
 */
public class FenzxpcjhTxtWriter extends DBOutputTxtSerivce{
	protected static Logger logger = Logger.getLogger(FenzxpcjhTxtWriter.class);	//定义日志方法
	public FenzxpcjhTxtWriter(DataParserConfig dataParserConfig) {
		
	}
	
	
	/**
	 * 接口完成后处理方法
	 * 根据创建时间删除100天以前的分装线计划
	 */
	@Override
	public void after() {
		try{
			dataParserConfig.getBaseDao().getSdcDataSource(sourceId).execute("outPut.deleteFenzxjh");
		}catch(RuntimeException e){
			logger.error("线程--接口" + interfaceId +"删除一个月前的分装线排产计划时报错"+e.getMessage());
		}
	}
}
