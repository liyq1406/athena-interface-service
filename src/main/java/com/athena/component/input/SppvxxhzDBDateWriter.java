package com.athena.component.input;

import org.apache.log4j.Logger;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.util.date.DateUtil;
import com.athena.util.exception.ServiceException;

/**
 * 3310 焊装过点信息
 * @date 2015-12-23
 * @author lc
 */
public class SppvxxhzDBDateWriter extends TxtInputDBSerivce{
    private String datasourceId1 = "";
    private String datasourceId2 = "";
	protected static Logger logger = Logger.getLogger(SppvxxhzDBDateWriter.class);	//定义日志方法

	public SppvxxhzDBDateWriter(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
		datasourceId1=dataParserConfig.getReaderConfig().getDatasourceId();
		datasourceId2=dataParserConfig.getWriterConfig().getDatasourceId();
	}
	
	
	/**
	 * 执行前将sppv.ATHENA006表中的STATE='F'改为'J'
	 * 2015-12-24
	 */
	@Override
	public void before(){
		try{
			logger.info("接口"+interfaceId+"调用SPPV存储过程更新STATE状态F->J开始，开始时间"+DateUtil.curDateTime());
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId1)
			.execute("inPutddbh.proc_updateATHENA006State_FtoJ");
			logger.info("接口"+interfaceId+"调用SPPV存储过程更新STATE状态F->J结束，结束时间"+DateUtil.curDateTime());			
		}catch(RuntimeException e){
			logger.error("sppv DatasourceId="+datasourceId1+"@线程--接口" + interfaceId +"SQL异常,before存储过程proc_updateATHENA006State_FtoJ更新sppv.ATHENA006表STATE状态F为J时报错"+e.getMessage());
			throw new ServiceException("sppv DatasourceId="+datasourceId1+"@线程--接口" + interfaceId +"SQL异常,before存储过程proc_updateATHENA006State_FtoJ更新sppv.ATHENA006表STATE状态F为J时报错"+e.getMessage());
		}
	}
	
	
	/**
	 * 接口处理完成后
	 * 1、更新SPPV ATHENA006表的状态为'T'(已处理状态)
	 * 2、更新ddbh_sppvxx set shengcx=usercenter||'2L'||substr(zongzlsh,1,1)
	 * 3、更新ddbh_sppvxx set flag='2'
	 * 4、将flag状态为2的记录写入到ddbh_sppvxx_zxc
	 * 5、更新ddbh_sppvxx set flag='3'
	 */
	@Override
	public void after() {
		try{
			//1、更新SPPV ATHENA006表的状态为'T'(已处理状态)
			logger.info("接口"+interfaceId+"调用SPPV存储过程更新STATE状态J->T开始，开始时间"+DateUtil.curDateTime());
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId1)
			.execute("inPutddbh.proc_updateAthena006State_JtoT");
			logger.info("接口"+interfaceId+"调用SPPV存储过程更新STATE状态J->T结束，结束时间"+DateUtil.curDateTime());
			//更新ddbh_sppvxx set shengcx=usercenter||'2L'||substr(zongzlsh,1,1)
			logger.info("更新ddbh_sppvxx set shengcx=usercenter||'2L'||substr(zongzlsh,1,1)");
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId2)
			.execute("inPutddbh.updatehzDdbh_sppvxx");
		    //3、更新ddbh_sppvxx set flag='2'
			//4、将flag状态为2的记录写入到ddbh_sppvxx_zxc
			//5、更新ddbh_sppvxx set flag='3'
			logger.info("接口"+interfaceId+"调用DDBH存储过程更新FLAG状态并写数据开始，开始时间"+DateUtil.curDateTime());
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId2)
			.execute("inPutddbh.proc_updateDdbhhzSppvxx");
			logger.info("接口"+interfaceId+"调用DDBH存储过程更新FLAG状态并写数据结束，结束时间"+DateUtil.curDateTime());
		}catch(RuntimeException e){
			logger.error("线程--接口" + interfaceId +"SQL异常,存储过程proc_updateDdbhhzSppvxx更新ddbh_sppvxx表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"SQL异常,存储过程proc_updateDdbhhzSppvxx更新ddbh_sppvxx表时报错"+e.getMessage());
		}
	}
}
