package com.athena.component.output;

import java.io.IOException;
import java.io.OutputStreamWriter;

import com.athena.component.DateTimeUtil;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.config.ExchangerConfig;
import com.athena.component.exchange.db.DBOutputTxtSerivce;
import com.athena.util.exception.ServiceException;

//lastModify 王冲, 2012-08-30 11:18
public class CpklszTxtWriter extends DBOutputTxtSerivce{
	public CpklszTxtWriter(DataParserConfig dataParserConfig) {

	}
	
	/**
	 * 增加空格
	 * @param num
	 * @return
	 */
	public static String getBlank(int num){
		String str="";
		for (int i = 0; i <= num; i++) {
			str+=" ";
		}
		return str;
	}

	/**
	 * 文件头记录的数据
	 */
	@Override
	public void fileBefore(OutputStreamWriter writer) {

		String DateTime=DateTimeUtil.getDateTimeStr("yyyyMMddHHmmss");
		try {
			writer.write("DEBATHENA  ath1osap04FS02492"+DateTime+""+getBlank(158)+"");
			writer.write("\n");
		} catch (IOException e) {
            logger.error("接口" + interfaceId + "IO输出异常", e);
            throw new ServiceException("接口" + interfaceId + "IO输出异常", e);
		}
		super.fileBefore(writer);
	}

	
	/**
	 * 文件尾记录的数据
	 */
	@Override
	public void fileAfter(ExchangerConfig write, ExchangerConfig read,OutputStreamWriter out) {
        try {
			out.write("FINATHENA  ath1osap04FS02492+"+totalToString()+""+getBlank(163)+"");
		} catch (IOException e) {
            logger.error("接口" + interfaceId + "IO输出异常", e);
            throw new ServiceException("接口" + interfaceId + "IO输出异常", e);
		}	
	}	

	public   String totalToString(){
		StringBuffer total = new StringBuffer(String.valueOf(this.getTotal()) ) ; 
		
		for(int i = total.length() ;i<9;i++){
			total.insert(0, "0") ;
		}
		
		return total.toString();
	}
	
	
}
