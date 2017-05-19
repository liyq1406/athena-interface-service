package com.athena.component.input;

import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.athena.component.DateTimeUtil;
import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.component.exchange.txt.WarmBusinessException;
import com.athena.util.date.DateUtil;
import com.athena.util.exception.ServiceException;


/**
 * 2047 EFI运输取货计划跟踪(输入)
 * @date 2016-11-22
 * @author lc
 */
public class EFIYsqhjhgzDBDataReader extends TxtInputDBSerivce{
	protected static Logger logger = Logger.getLogger(EFIYsqhjhgzDBDataReader.class);	//定义日志方法 
	public Date date = new Date();
	int num = 1;

	public EFIYsqhjhgzDBDataReader(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
	}
	
	/**
	 * 解析数据之前清空ck_quhjhgz_tmp表数据
	 */
	@Override
	public void before() {
		try{
			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
	                .execute("inPutzxc.qhjhgz_tmpDelete");
		}catch(RuntimeException e){
			logger.error("线程--接口" + interfaceId +"清除ck_quhjhgz_tmp表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"清除ck_quhjhgz_tmp表时报错"+e.getMessage());
		}
	}
	
	/**
	 * 行解析之后处理方法
	 * 给行记录增加到达物理点时间、流水号、创建人、创建时间、修改人、修改时间
	 */
	@Override
	public boolean afterRecord(Record record) {		
		String daodwldsj = "";
		String yingdsj = DateTimeUtil.SubString(record.getString("yingdsj").trim());
		String pianc = record.getString("pianc").trim();
		if(null!=yingdsj&&!"".equals(yingdsj)){
			if(null==pianc || "".equals(pianc)){
				pianc = "0";
			}
			int pc = Integer.parseInt(pianc);
			daodwldsj = DateUtil.dateAddMinutes(yingdsj,pc);
			try {
				record.put("yingdsj",DateTimeUtil.StringYMDToDate(yingdsj));
				record.put("daodwldsj",DateTimeUtil.StringYMDToDate(daodwldsj));
			} catch (ParseException e) {
				logger.error("线程--接口" + interfaceId + "第" + record.getLineNum() + "行数据时间转换异常  " + e.getMessage());
				throw new WarmBusinessException("日期转换错误！"+e.getMessage());
			}
		}				
		record.put("liush",num);
		record.put("create_time",date);
		record.put("creator",interfaceId);		
		record.put("edit_time",date);
		record.put("editor","temp");
		num++;
		return true;
	}
	
	/**
	 * 接口完成后处理方法 
	 * 将CK_QUHJHGZ_TMP表数据插入或更新到CK_QUHJHGZ表中
	 * 更新ck_daohtzd表中的卡车号
	 */
	@SuppressWarnings("unchecked")
	public void after() {
		try{
			//查询CK_QUHJHGZ_TMP表中重复的运输计划号
			List<Map<String, Object>> listYunsjhh = dataParserConfig.getBaseDao()
			        .getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId()).select("inPutzxc.queryByQuhjhgz_tmp");
			//根据运输状态和最晚上传时间判断重复运输计划号的有效性，将无效的打标
			for (Map<String, Object> cxmap : listYunsjhh) {
				String yszt = cxmap.get("YUNSZT").toString();
				if("3".equals(yszt)){//当最大运输状态为3时，取运输状态为3的最早的一个上传时间为有效，将无效的打标
					dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
					        .execute("inPutzxc.updateByYunszt", cxmap);
				}else{//当最大运输状态不为3时，取最晚的一个上传时间为有效，将无效的打标
					dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			                .execute("inPutzxc.updateByShangcsj", cxmap);
				}			
			}
		}catch(RuntimeException e){
			logger.error("线程--接口" + dataParserConfig.getId() +"根据运输状态和最晚上传时间判断重复运输计划号的有效性，将无效的打标报错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"根据运输状态和最晚上传时间判断重复运输计划号的有效性，将无效的打标报错"+e.getMessage());
		}
		try{
			//Merge表CK_QUHJHGZ数据
			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			        .execute("inPutzxc.ck_quhjhgzMerge");
		}catch(RuntimeException e){
			logger.error("线程--接口" + dataParserConfig.getId() +"Merge表CK_QUHJHGZ时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"Merge表CK_QUHJHGZ时报错"+e.getMessage());
		}
		try{
			//更新ck_daohtzd表中的卡车号
			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			        .execute("inPutzxc.updateck_daohtzdkch");
			//将editor为temp的更新为2047
			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			        .execute("inPutzxc.updateqhjhgzeditor");
		}catch(RuntimeException e){
			logger.error("线程--接口" + dataParserConfig.getId() +"更新ck_daohtzd表中的卡车号报错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"更新ck_daohtzd表中的卡车号报错"+e.getMessage());
		}		
	}
}
