package com.athena.component.input;

import org.apache.log4j.Logger;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.util.exception.ServiceException;

/**
 * 2480 DDBH拆分结果CS
 * @author 贺志国
 * @E-mail zghe@isoftstone.com
 * @version v1.0
 * @date 2013-12-10
 */
public class ChaifjsCSDBDataReader extends TxtInputDBSerivce {
	protected static Logger logger = Logger.getLogger(ChaifjsCSDBDataReader.class);
	private String datasourceId = null;
	public ChaifjsCSDBDataReader(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
		datasourceId = dataParserConfig.getWriterConfig().getDatasourceId();
	}
	
	/**
	 * 处理之前执行清表动作
	 * 清除中间表数据in_caifjg_cs
	 * hzg 2013-12-17
	 */
	@Override
	public void before(){
		dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzxc.truncateOfin_caifjg_cs");
	}
	
	
	
	/**
	 * 处理后的调用
	 * 更新XIAOHDPYL, ZHENGCXH ,XIAOHCBH	 ,MOS四个值
	 * 修改：整车序号按整体数据来排，不仅仅按CS的来排，排完后，再更新MOS，取MOS='CS'的数据进行处理。
	 * 修改日期：2013.12.20 
	 * 修改日期：2013.12.23
	 */
	@Override
	public void after(){
        try{
        	
        	//1、更新MOS的值为'CS' hzg 2013.12.23
    		dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzxc.updateMosOfChaifjg_cs");	
        	//2、将中间表中in_caifjg_cs表中模式MOS为CS的导入到ddbh_caifjg_cs业务表   hzg 2013.12.23
        	dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzxc.insertInCaifjgCsToDdbhCaifjsCs");
        	
        	//3、更新6000点ZHENGCXH值 ,根据产线和总装流水号来做更新  ，更新业务表  hzg 2013.12.23 和胡确认过每版数据不会有相同的，如果有相同的数据此处会抛出主键冲突ORA-00001 
        		dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzxc.updateZhengcxhOfChaifjg_cs");
        	//4、更新1点和1000点ZHENGCXH值 ,根据产线和总装流水号来做更新  ，更新业务表  hzg 2013.12.23 和胡确认过每版数据不会有相同的，如果有相同的数据此处会抛出主键冲突ORA-00001 
        		dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzxc.updateZhengcxhOfChaifjg_cs1");
        	//5、更新XIAOHCBH值 ,带CS条件
        		dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzxc.updateXiaohcbhOfChaifjg_cs");
        	//6、更新XIAOHDPYL值,带CS条件
        		dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzxc.updateXiaohdpylOfChaifjg_cs");
        	//7、将所有更新后的数据并且不为空的记录的FLAG标识更新为0
        		dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzxc.updateFlagToZeroOfNotNull");
        }catch(RuntimeException e){
			logger.error("主线程--接口" + interfaceId +"更新ddbh_caifjg_cs表时报错"+e.getMessage());
			throw new ServiceException("主线程--接口" + interfaceId +"更新ddbh_caifjg_cs表时报错"+e.getMessage());
		}
	}
	
}
