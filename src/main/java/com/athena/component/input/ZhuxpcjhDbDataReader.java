package com.athena.component.input;

import java.util.Date;

import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;


/**
 * 焊装排产计划 3240
 * @date 2015-11-17
 * @author yz
 */
public class ZhuxpcjhDbDataReader extends TxtInputDBSerivce{
	private  Date date=new Date();
	//需求来源默认值为1；
	private String xuqly = "1";
		
	public ZhuxpcjhDbDataReader(DataParserConfig dataParserConfig) {
		super(dataParserConfig);		
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
	 * 给行记录增加创建人、创建时间、修改人、修改时间
	 * 需求来源默认值设置为1
	 * 大线线号为两位则为用户中心+2+大线线号
	 * 5位则不变
	 * 展开日期读8位写10位
	 */
	@Override
	public boolean afterRecord(Record record) {
		String usercenter = record.getString("usercenter").trim();
		String zhankrq = record.getString("zhankrq").trim();
		zhankrq = zhankrq.substring(0,4)+"-"+zhankrq.substring(4,6)+"-"+zhankrq.substring(6);
		String daxxh = record.getString("daxxh").trim();
		if(daxxh.length()==2){
			record.put("daxxh",usercenter+2+daxxh);
		}else{
			record.put("daxxh",daxxh);
		}
		record.put("zhankrq",zhankrq);
		record.put("xuqly",xuqly);
		record.put("create_time",date);
		record.put("creator",interfaceId);
		return true;
	}
	
	/**
	 * 接口完成后处理方法
	 * 根据创建时间删除100天以前的主线计划
	 */
	@Override
	public void after() {
		try{
			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.execute("inPutddbh.deleteZhuxjh");		
		}catch(RuntimeException e){
			logger.error("线程--接口" + interfaceId +"删除一个月前的的主线计划计划时报错"+e.getMessage());
		}
	}

	
	
	
}
