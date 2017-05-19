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
 * 1560 PDS零件消耗点输出
 * @author 贺志国
 * @E-mail zghe@isoftstone.com
 * @version v1.0
 * @date 2014-4-3
 */
public class XhdljTxtWriter extends DBOutputTxtSerivce{
	protected static Logger logger = Logger.getLogger(XhdljTxtWriter.class);	//定义日志方法
	public XhdljTxtWriter(DataParserConfig dataParserConfig) {
		
	}
	@Override
	public void fileBefore(OutputStreamWriter writer) {
		String BeginTime=DateTimeUtil.getDateTimeStr("yyyyMMdd HH:mm:ss");
		try {
			writer.write(""+BeginTime+" ATHENA—PDS INTERFACE04 BEGIN==>");
			writer.write("\n");
		} catch (IOException e) {
            logger.error("接口" + interfaceId + "IO输出异常", e);
            throw new ServiceException("接口" + interfaceId + "IO输出异常", e);
		}
		
	}

	public void fileAfter(ExchangerConfig write, ExchangerConfig read,OutputStreamWriter out) {
		String EndTime=DateTimeUtil.getDateTimeStr("yyyyMMdd HH:mm:ss");
        try {
        	out.write(""+EndTime+" ATHENA—PDS INTERFACE04 RECORDS="+this.getTotal()+" END<==");
		} catch (IOException e) {
            logger.error("接口" + interfaceId + "IO输出异常", e);
            throw new ServiceException("接口" + interfaceId + "IO输出异常", e);
		}
	}
	
	
	/**
	 * 1560输出完成后，将ckx_lingjxhd_s表结束日期小于当前日期的数据失效
	 * hzg 2014.4.3
	 */
	@Override
	public void after(){
		//将ckx_lingjxhd_s表结束日期小于当前日期的数据失效
		baseDao.getSdcDataSource(sourceId).execute("outPut.updateBiaosOfLingjxhd_s");
	}
	

}
