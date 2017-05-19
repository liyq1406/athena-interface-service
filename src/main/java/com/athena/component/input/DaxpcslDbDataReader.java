package com.athena.component.input;

import java.util.Date;

import org.apache.log4j.Logger;

import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;


/**
 * 焊装排产数量 3250
 * @date 2015-11-27
 * @author yz
 */
public class DaxpcslDbDataReader extends TxtInputDBSerivce{
	private  Date date=new Date();

	protected static Logger logger = Logger.getLogger(DaxpcslDbDataReader.class);	//定义日志方法
	
	public DaxpcslDbDataReader(DataParserConfig dataParserConfig) {
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
	 * 给行记录增加创建人、创建时间
	 * 当计划上线量和计划下线量为空或者不是数字的时候设为0；
	 */
	@Override
	public boolean afterRecord(Record record) {
		String jihsxl = record.getString("jihsxl").trim();
		if(jihsxl.matches("[0-9]+")){
			record.put("jihsxl", jihsxl);
		}else{
			record.put("jihsxl", 0);
		}
		String jihxxl = record.getString("jihxxl").trim();
		if(jihxxl.matches("[0-9]+")){
			record.put("jihxxl", jihxxl);
		}else{
			record.put("jihxxl", 0);
		}
		record.put("editor",interfaceId);
		record.put("edit_time",date);
		record.put("creator",interfaceId);
		record.put("create_time",date);	
		//2016-06-02对传入的大线线号进行判断，如果大线线号长度为 2位，则取其3位车间号与大线线号拼接成新大线线号
		String daxxh = record.getString("daxxh").trim();
		if (daxxh.length() == 2) {
			record.put("daxxh", record.getString("chej").trim()+daxxh);
		}
		return true;
	}
}
