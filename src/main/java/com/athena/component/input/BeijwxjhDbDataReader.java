package com.athena.component.input;

import java.util.Date;

import org.apache.log4j.Logger;

import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.util.exception.ServiceException;


/**
 * 备件外销计划(输入) 3260
 * @date 2015-11-30
 * @author lc
 */
public class BeijwxjhDbDataReader extends TxtInputDBSerivce{
	private  Date date = new Date();
	int num = 1;
	private String lsh = "";
	private String datasourceId = "";
	protected static Logger logger = Logger.getLogger(BeijwxjhDbDataReader.class);	//定义日志方法
	
	public BeijwxjhDbDataReader(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
		datasourceId = dataParserConfig.getWriterConfig().getDatasourceId();
		lsh = (String)dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).selectObject("inPutddbh.queryliush");
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
	 * 行解析之后处理方法
	 * 给行记录更新展开日期，增加需求来源、流水号、完成数量、创建人、创建时间
	 * 展开日期八位改为十位
	 * 需求来源默认值设置为1
	 * 流水号值每解析一行加1
	 * 完成数量默认值设置为0
	 */
	@Override
	public boolean afterRecord(Record record) {	
		String zhankrq = record.getString("zhankrq").trim();
		zhankrq = zhankrq.substring(0,4)+"-"+zhankrq.substring(4,6)+"-"+zhankrq.substring(6);
		record.put("zhankrq",zhankrq);
		record.put("xuqly","1");
		record.put("liush",num+Integer.parseInt(lsh));
		record.put("wancsl",0);
		record.put("create_time",date);
		record.put("creator",interfaceId);	
		num++;
		return true;
	}
	
	/**
	 * 接口完成后处理方法
	 * 关联备件外销计划表和分装线表，得到大线线号
	 */
	@Override
	public void after() {
		try{
			//关联备件外销计划表和分装线表，得到大线线号
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutddbh.updateBeijwxjh");
			//若更新后大线线号仍为空值，则取分装线号值更新
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutddbh.updatedaxxh");
		}catch(RuntimeException e){
			logger.error("线程--接口" + dataParserConfig.getId() +"更新in_beijwxjh表大线线号报错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"更新in_beijwxjh表大线线号报错"+e.getMessage());
		}					
	}
	
}
