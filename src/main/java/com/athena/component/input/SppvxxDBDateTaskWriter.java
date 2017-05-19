package com.athena.component.input;
import java.util.List;

import org.apache.log4j.Logger;

import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtWriterDBTask;

/**
 *3020 SPPV整车过点信息输入类
 * @author HZG
 * @date 2014-4-15
 */
public class SppvxxDBDateTaskWriter extends TxtWriterDBTask {
	protected static Logger logger = Logger.getLogger(SppvxxDBDateTaskWriter.class);	//定义日志方法 
   
	public SppvxxDBDateTaskWriter(DataParserConfig dataParserConfig,
			List<String> fieldList, List<String> updateFieldList,
			String fileName, List<Record> lineList) {
		super(dataParserConfig, fieldList, updateFieldList, fileName, lineList); 
	}

	

	/**
	 * 行解析之后处理方法
	 * @param rowIndex 行标
	 * @param record 行数据集合
	 * @author GJ
	 * @update hzg 2012-10-11  直接写业务表
	 */
	@Override
	public  boolean beforeRecord(Record record) {
		String wuld = record.getString("WULD");//取sppv物理点
		String usercenter = record.getString("USERCENTER");//取sppv用户中心
		String zongzlsh = record.getString("ZONGZLSH");//取sppv总装流水号
		String shengcx = "";
		//判断是6000点还是2000点
		if("6000".equals(wuld)){//6000点数据
			shengcx = usercenter.concat("5L").concat(zongzlsh.substring(0, 1));
		}else{ //2000点数据
			shengcx = usercenter.concat("2L").concat(zongzlsh.substring(0, 1));
		}
		record.put("SHENGCX", shengcx);
		return true;
	}
}
