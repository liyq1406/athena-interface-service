package com.athena.component.output;
import java.io.IOException;
import java.io.OutputStreamWriter;

import com.athena.component.DateTimeUtil;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.config.ExchangerConfig;
import com.athena.component.exchange.db.DBOutputTxtSerivce;
import com.athena.util.exception.ServiceException;

public class LjkckzTxtWriter extends  DBOutputTxtSerivce{
	public LjkckzTxtWriter(DataParserConfig dataParserConfig) {
	}

	/**
	 * 生成文件头 
	 *  文件头格式固定：
	 *  	DEB  HERMES  PSA       SE0559420120927014525
	 * @throws IOException 
	 */
	@Override
	public void fileBefore(OutputStreamWriter writer) {
		String DateTime=DateTimeUtil.getDateTimeStr("yyyyMMddHHmmss");
        try {
			writer.write("DEB  HERMES  PAS       SE05594"+DateTime+"        ");
			writer.write("\n");
		} catch (IOException e) {
            logger.error("接口" + interfaceId + "IO输出异常", e);
            throw new ServiceException("接口" + interfaceId + "IO输出异常", e);
		}	
		 super.fileBefore(writer);
	}
    

	/**
	 * 生成文件尾 
	 *  文件尾格式固定：
	 *  	FIN  HERMES  PSA       SE05594000000016
	 * @throws IOException 
	 */
	@Override
	public void fileAfter(ExchangerConfig write, ExchangerConfig read,OutputStreamWriter out) {
        try {
			out.write("FIN  HERMES  PAS       SE05594"+totalToString()+"             ");
			
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
   