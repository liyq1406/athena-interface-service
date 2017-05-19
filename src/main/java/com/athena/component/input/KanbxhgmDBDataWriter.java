package com.athena.component.input;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtWriterDBTask;
/**
 * 2070 看板循环规模
 * @author kong
 *
 */
public class KanbxhgmDBDataWriter  extends TxtWriterDBTask{
	public KanbxhgmDBDataWriter(DataParserConfig dataParserConfig, List<String> fieldList, List<String> updateFieldList, String fileName, List<Record> lineList) {
		super(dataParserConfig, fieldList, updateFieldList, fileName, lineList); 
	}
	Date date=new Date();
	SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	/**
	 * 行记录解析之后   给行记录增加创建人、创建时间、修改人、修改时间
	 * */
	@Override
	public boolean beforeRecord(Record record) {	
		record.put("create_time",Timestamp.valueOf(df.format(date)));
		record.put("creator",interfaceId);
		record.put("EDIT_TIME",Timestamp.valueOf(df.format(date)));
		record.put("EDITOR",interfaceId);
		record.put("weihsj",StringUtils.isEmpty(record.getString("weihsj").trim())?null:Timestamp.valueOf(record.getString("weihsj")));
		// Mantis 3991 当前循环规模值（DANGQXHGM）不从准备层同步  将已下发的循环规模覆盖当前循环规模-->
		//record.put("dangqxhgm",record.getString("xiafxhgm"));
		//record.put("dangqxhgm","");
		String time=record.getString("jissj");
		record.put("jissj",StringUtils.isEmpty(time.trim())?null:Timestamp.valueOf(time));
		return true;
	}
}
