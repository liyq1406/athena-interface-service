package com.athena.component.output;

import java.io.IOException;
import java.io.OutputStreamWriter;
import org.apache.log4j.Logger;

import com.athena.component.DateTimeUtil;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.config.ExchangerConfig;
import com.athena.component.exchange.db.DBOutputTxtSerivce;
import com.athena.util.exception.ServiceException;

/**
 * 3290 需拆分零件消耗点输出
 * @author 吕婵
 * @date 2015-11-23
 */
public class XucfljxhdTxtWriter extends DBOutputTxtSerivce{
	protected static Logger logger = Logger.getLogger(XucfljxhdTxtWriter.class);	//定义日志方法
	public XucfljxhdTxtWriter(DataParserConfig dataParserConfig) {
		
	}
	@Override
	public void fileBefore(OutputStreamWriter writer) {
		String BeginTime=DateTimeUtil.getDateTimeStr("yyyy-MM-dd HH:mm:ss");
		try {
			writer.write("("+BeginTime+" ATHENA—PDS INTERFACE06 BEGIN==>"+")");
			writer.write("\n");
		} catch (IOException e) {
            logger.error("接口" + interfaceId + "IO输出异常", e);
            throw new ServiceException("接口" + interfaceId + "IO输出异常", e);
		}
		
	}
    
	@Override
	public void fileAfter(ExchangerConfig write, ExchangerConfig read,OutputStreamWriter out) {
		String EndTime=DateTimeUtil.getDateTimeStr("yyyy-MM-dd HH:mm:ss");
        try {
        	out.write("("+EndTime+" ATHENA—PDS INTERFACE06 RECORDS="+this.getTotal()+" END<=="+")");
		} catch (IOException e) {
            logger.error("接口" + interfaceId + "IO输出异常", e);
            throw new ServiceException("接口" + interfaceId + "IO输出异常", e);
		}
	}
}
