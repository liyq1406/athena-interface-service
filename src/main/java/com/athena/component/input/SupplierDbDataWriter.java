package com.athena.component.input;

import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtWriterDBTask;


/**
 * 供应商参考系接口输入类
 * @author HZG
 * @update hzg 2013-1-31。
 */
public class SupplierDbDataWriter extends TxtWriterDBTask{
	protected static Logger logger = Logger.getLogger(SupplierDbDataWriter.class);	//定义日志方法
	public SupplierDbDataWriter(DataParserConfig dataParserConfig,
			List<String> fieldList, List<String> updateFieldList,
			String fileName, List<Record> lineList) {
		super(dataParserConfig, fieldList, updateFieldList, fileName, lineList); 
	}

	/**
	 * 行解析之后处理方法
	 * @param rowIndex 行标
	 * @param record 行数据集合
	 * @author HZG
	 */
	@Override
	public boolean beforeRecord(Record record) {
			//插入创建时间和处理状态初始数据
			record.put("leix", "2");
			record.put("fayd", "GYS");
			record.put("biaos", "1");
			record.put("gonghlx", "97W");
			record.put("creator", interfaceId) ;
			record.put("editor", interfaceId) ;
			record.put("create_time", new Date()) ;
			record.put("edit_time", new Date()) ;
			return true;
	}
}
