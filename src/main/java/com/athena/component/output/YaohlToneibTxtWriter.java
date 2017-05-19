package com.athena.component.output;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.db.DBOutputTxtSerivce;

public class YaohlToneibTxtWriter extends  DBOutputTxtSerivce{
	public YaohlToneibTxtWriter(DataParserConfig dataParserConfig) {
	}

	/**
	 * 运行前更新beiz2的状态为0
	 */
	@Override
	public void before() {
		int num = baseDao.getSdcDataSource(sourceId).execute("outPut.updateyaohltoneibbegin");
		logger.info("线程--接口" + interfaceId +" 更新数据要货令条数为："+num);
	}
    

	/**
	 * 运行前更新beiz2的状态为1
	 */
	@Override
	public void after() {
		int num = baseDao.getSdcDataSource(sourceId).execute(
					"outPut.updateyaohltoneibEnd");
			logger.info("线程--接口" + interfaceId +" 更新数据要货令条数为："+num);	
	}

}
   