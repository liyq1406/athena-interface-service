package com.athena.component.input;

import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;


/**
 * 3490入库明细
 * @date 2015-12-18
 * @author yz
 */
public class RukmxDbDataReader extends TxtInputDBSerivce{
	//备注默认为0
	private String beiz = "0";
	public  Date date=new Date();
	protected static Logger logger = Logger.getLogger(RukmxDbDataReader.class);	//定义日志方法
	
	public RukmxDbDataReader(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
		
	}
		
	/**
	 * 行解析之后处理方法
	 * 给行记录增加创建人、创建时间、修改人、修改时间
	 * 备注1备注2为空则默认为0
	 */
	@Override
	public boolean afterRecord(Record record) {
		String beiz1 = record.getString("BEIZ1").trim();
		if(StringUtils.isEmpty(beiz1)){
			record.put("BEIZ1", beiz);
		}
		String beiz2 = record.getString("BEIZ2").trim();
		if(StringUtils.isEmpty(beiz2)){
			record.put("BEIZ2", beiz);
		}
		record.put("create_time",date);
		record.put("creator",interfaceId);
		record.put("editor",interfaceId);
		record.put("edit_time",date);
		return true;
	}
		
	
}
