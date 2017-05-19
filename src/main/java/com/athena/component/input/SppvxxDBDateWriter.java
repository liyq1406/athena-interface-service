package com.athena.component.input;

import org.apache.log4j.Logger;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.util.date.DateUtil;
import com.athena.util.exception.ServiceException;

/**
 * 3020 整车过点信息
 * @date 2013-3-15
 * @author hzg
 * @update hzg 2014-5-19 改为存储过程处理 
 */
public class SppvxxDBDateWriter extends TxtInputDBSerivce{
    private String datasourceId1 = "";
    private String datasourceId2 = "";   
	protected static Logger logger = Logger.getLogger(SppvxxDBDateWriter.class);	//定义日志方法

	public SppvxxDBDateWriter(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
		datasourceId1=dataParserConfig.getReaderConfig().getDatasourceId();
		datasourceId2=dataParserConfig.getWriterConfig().getDatasourceId();
	}
	
	/**
	 * 执行前将sppv.ATHENA001表中的STATE='F'改为'J' hzg  mantis:8600
	 * 2013-10-12
	 */
	@Override
	public void before(){
		try{
			logger.info("接口"+interfaceId+"调用SPPV存储过程更新STATE状态F->J开始，开始时间"+DateUtil.curDateTime());
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId1)
			.execute("inPutddbh.proc_updateATHENA001State_FtoJ");
			logger.info("接口"+interfaceId+"调用SPPV存储过程更新STATE状态F->J结束，结束时间"+DateUtil.curDateTime());
		}catch(RuntimeException e){
			logger.error("sppv DatasourceId="+datasourceId1+"@线程--接口" + interfaceId +"SQL异常,before存储过程proc_updateATHENA001State_FtoJ更新sppv.ATHENA001表STATE状态F为J时报错"+e.getMessage());
			throw new ServiceException("sppv DatasourceId="+datasourceId1+"@线程--接口" + interfaceId +"SQL异常,before存储过程proc_updateATHENA001State_FtoJ更新sppv.ATHENA001表STATE状态F为J时报错"+e.getMessage());
		}
	}
	
	
	/**
	 * 接口处理完成后1、更新SPPV ATHENA001表的状态为'T'(已处理状态)
	 * 2、更新ddbh_sppvxx set shengcx=usercenter||'5L'||substr(zongzlsh,1,1) 
	 * 3、更新ddbh_sppvxx set shengcx=usercenter||'2L'||substr(zongzlsh,1,1)
	 * 4、更新ddbh_sppvxx set flag='2'
	 * 5、将flag状态为2的记录写入到ddbh_sppvxx_zxc
	 * 6、更新ddbh_sppvxx set flag='3'
	 */
	@Override
	public void after() {
		try{
			logger.info("接口"+interfaceId+"调用SPPV存储过程更新STATE状态J->T开始，开始时间"+DateUtil.curDateTime());
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId1)
			.execute("inPutddbh.proc_updateAthena001State_JtoT");
			logger.info("接口"+interfaceId+"调用SPPV存储过程更新STATE状态J->T结束，结束时间"+DateUtil.curDateTime());
			/*
			 logger.info("更新SPPV ATHENA001表的状态为'T'(已处理状态)");
			  dataParserConfig.getBaseDao().getSdcDataSource(datasourceId2)
			.execute("inPutddbh.updateDdbh_sppvxx1");
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId2)
			.execute("inPutddbh.updateDdbh_sppvxx2");*/
			//add by hzg 2014.4.12
			/*logger.info("更新ddbh_sppvxx表flag状态为2");
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId2)
			.execute("inPutddbh.updateFlagOfSppvxx1");
			logger.info("将flag状态为2的整车过点信息写到ddbh_sppvxx_zxc表");
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId2)
			.execute("inPutddbh.insertSppvxxToSppvzxc");
			logger.info("更新ddbh_sppvxx表flag状态为3");
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId2)
			.execute("inPutddbh.updateFlagOfSppvxx2");*/
			logger.info("接口"+interfaceId+"调用DDBH存储过程更新FLAG状态并写数据开始，开始时间"+DateUtil.curDateTime());
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId2)
			.execute("inPutddbh.proc_updateDdbhSppvxx");
			logger.info("接口"+interfaceId+"调用DDBH存储过程更新FLAG状态并写数据结束，结束时间"+DateUtil.curDateTime());

		}catch(RuntimeException e){
			logger.error("线程--接口" + interfaceId +"SQL异常,存储过程proc_updateDdbhSppvxx更新ddbh_sppvxx表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"SQL异常,存储过程proc_updateDdbhSppvxx更新ddbh_sppvxx表时报错"+e.getMessage());
		}
	}

}
