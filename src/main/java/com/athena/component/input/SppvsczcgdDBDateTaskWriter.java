package com.athena.component.input;

import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtWriterDBTask;


/**
 * 2650,2651,2950 SPPV整车过点信息
 * @author LC
 * @date 2016-6-16
 */
public class SppvsczcgdDBDateTaskWriter extends TxtWriterDBTask {
	protected static Logger logger = Logger.getLogger(SppvsczcgdDBDateTaskWriter.class);	//定义日志方法 
	private Date date= new Date();
	
	public SppvsczcgdDBDateTaskWriter(DataParserConfig dataParserConfig,
			List<String> fieldList, List<String> updateFieldList,
			String fileName, List<Record> lineList) {
		super(dataParserConfig, fieldList, updateFieldList, fileName, lineList); 
	}
	

	/**
	 * 行解析之后处理方法
	 * 给行记录增加创建人、创建时间，并将时间类型数据格式转化成【yyyy-MM-dd HH:mm:ss】形式
	 * @param rowIndex 行标
	 * @param record 行数据集合
	 */
	@Override
	public boolean beforeRecord(Record record) {
		String wuld = record.getString("WULD");//取sppv物理点
		int i_wuld = Integer.parseInt(wuld);//将物理点转换为整型
		String usercenter = record.getString("USERCENTER");//取sppv用户中心
		String zongzlsh = record.getString("ZONGZLSH");//取sppv总装流水号
		String shengcx = "";
		//判断是总装、涂装还是焊装
		if(i_wuld<50){//物理点小于50  焊装分装线
			shengcx = usercenter.concat("2F").concat(zongzlsh.substring(0, 1));
		}else if(i_wuld>=50&&i_wuld<1000){//物理点大于等于50小于1000  总装分装线
			shengcx = usercenter.concat("5M").concat(zongzlsh.substring(0, 1));
		}else if(i_wuld>=1000&&i_wuld<3000){//物理点大于等于1000小于3000   焊装大线
			shengcx = usercenter.concat("2L").concat(zongzlsh.substring(0, 1));
		}else if(i_wuld>=3000&&i_wuld<6000){//物理点大于等于3000小于6000  涂装大线
			shengcx = usercenter.concat("3L").concat(zongzlsh.substring(0, 1));
		}else if(i_wuld>=6000){//物理点大于等于6000  总装大线
			shengcx = usercenter.concat("5L").concat(zongzlsh.substring(0, 1));
		}
		record.put("SHENGCX", shengcx);
		record.put("CREATOR", interfaceId);
		record.put("CREATE_TIME", date);
		return true;
	}
}
