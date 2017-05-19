package com.athena.component.input;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.db.DbDataWriter;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.util.exception.ServiceException;


/**
 * 2045 EFI运输取货计划(输入)
 * @date 2016-11-18
 * @author lc
 */
public class EFIYsqhjhDBDataReader extends TxtInputDBSerivce{
	protected static Logger logger = Logger.getLogger(EFIYsqhjhDBDataReader.class);	//定义日志方法 
	public Date date = new Date();
	private String logInfo = "";
	private String logDate = "";

	public EFIYsqhjhDBDataReader(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
	}
	
	/**
	 * 行解析之后处理方法
	 * 给行记录增加创建人、创建时间、修改人、修改时间
	 */
	@Override
	public boolean afterRecord(Record record) {		
		record.put("create_time",date);
		record.put("creator","2045");		
		record.put("edit_time",date);
		record.put("editor","temp");
		return true;
	}
	
	/**
	 * 接口完成后处理方法 
	 * 1、删除该版次中新插入的在表中原已存在的运输计划号下的所有数据，并记错误日志写入in_errorfile表
	 * 2、删除该版次中新插入的在表中重复的要货令号对应的运输计划号下的所有数据，并记错误日志写入in_errorfile表
	 * 3、查询得到零件编号、零件数量、目的地、预计到货时间、产线、仓库编号
	 */
	@SuppressWarnings("unchecked")
	public void after() {
		try{
			//查询新插入的运输计划号在表中是否原已存在
			List<Map<String, Object>> listYunsjhh = dataParserConfig.getBaseDao()
			        .getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId()).select("inPutzxc.queryGroupbyYunsjhh");
			//删除该版次中新插入的在表中原已存在的运输计划号下的所有数据，并记错误日志写入in_errorfile表
			for (Map<String, Object> cxmap : listYunsjhh) {
				String SID=DbDataWriter.getUUID();//唯一标示
				String EID=DbDataWriter.getUUID();//唯一标示
				dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
						.execute("inPutzxc.deleteGroupbyYunsjhh", cxmap);
				logger.error("线程--接口" + dataParserConfig.getId() + " 运输计划号：" + cxmap.get("YUNSJHH").toString() + "在ck_quhjh表中已存在，过滤掉该版次中该运输计划号下的所有数据");
				logInfo = "运输计划号：" + cxmap.get("YUNSJHH").toString() + "在ck_quhjh表中已存在，过滤掉该版次中该运输计划号下的所有数据";
				File_ErrorInfo(EID, "2045", SID, logInfo, logDate);
			}
			//查询新插入的要货令号在表中是否重复
			List<Map<String, Object>> listYaohlh = dataParserConfig.getBaseDao()
			        .getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId()).select("inPutzxc.queryGroupbyYaohlh");
			//删除该版次中新插入的在表中重复的要货令号对应的运输计划号下的所有数据，并记错误日志写入in_errorfile表
			for (Map<String, Object> xhmap : listYaohlh) {
				String SID=DbDataWriter.getUUID();//唯一标示
				String EID=DbDataWriter.getUUID();//唯一标示
				dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
						.execute("inPutzxc.deleteGroupbyYunsjhh", xhmap);
				logger.error("线程--接口" + dataParserConfig.getId() + " 运输计划号：" + xhmap.get("YUNSJHH").toString() + "下的要货令号在ck_quhjh表中存在重复值，过滤掉该版次中该运输计划号下的所有数据");
				logInfo = "运输计划号：" + xhmap.get("YUNSJHH").toString() + "下的要货令号在ck_quhjh表中存在重复值，过滤掉该版次中该运输计划号下的所有数据";
				File_ErrorInfo(EID, "2045", SID, logInfo, logDate);
			}
		}catch(RuntimeException e){
			logger.error("线程--接口" + dataParserConfig.getId() +"写in_errorfile表报错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"写in_errorfile表报错"+e.getMessage());
		}
		
		try{			
			//查询得到零件编号、零件数量、目的地、预计到货时间、产线、仓库编号
			List<Map<String, Object>> quhjhList = dataParserConfig.getBaseDao()
					.getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId()).select("inPutzxc.queryquhjh");
			//更新零件编号、零件数量、目的地、预计到货时间、产线、仓库编号
			for (Map<String, Object> map : quhjhList) {
				dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
						.execute("inPutzxc.updatequhjh", map);
			}
			//将editor为temp的更新为2045
			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			        .execute("inPutzxc.updateqhjheditor");
		}catch(RuntimeException e){
			logger.error("线程--接口" + dataParserConfig.getId() +"更新ck_quhjh表零件编号、零件数量、目的地、预计到货时间、产线、仓库编号报错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"更新ck_quhjh表零件编号、零件数量、目的地、预计到货时间、产线、仓库编号报错"+e.getMessage());
		}		
	}
	
	/**
	 * 记录数据日志表
	 */
	public void File_ErrorInfo(String EID,String CID,String SID,String file_errorinfo,String error_date){
		Map<String,String> params = new HashMap<String,String>();
		params.put("EID", strNull(EID));
		params.put("INBH", strNull(CID));
		params.put("SID", strNull(SID));
		params.put("file_errorinfo", strNull(file_errorinfo));
		params.put("error_date", strNull(error_date));
		try {
			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			        .execute("inPutzxc.insertErrorFileInfo",params);
		}catch(RuntimeException e){
			logger.error("线程--接口" + dataParserConfig.getId() +"写in_errorfile表报错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"写in_errorfile表报错"+e.getMessage());
		}
	}
	
	/**
	 * 空串处理
	 * @param obj 对象
	 * @return 处理后字符串
	 * @date 2011-10-26
	 */
	private String strNull(Object obj) {// 对象为空返回空串,不为空toString
		return obj == null ? "" : obj.toString().trim();
	}
}
