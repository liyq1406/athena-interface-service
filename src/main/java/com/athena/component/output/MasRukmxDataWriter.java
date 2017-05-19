package com.athena.component.output;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Date;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.db.DBOutputTxtSerivce;
import com.athena.util.date.DateUtil;
import com.athena.util.exception.ServiceException;

/**
 * 2830保税库 A01 入库明细
 * @author 贺志国
 * @E-mail zghe@isoftstone.com
 * @version v1.0
 * @date 2013-12-31
 */
public class MasRukmxDataWriter extends DBOutputTxtSerivce {
	public MasRukmxDataWriter(DataParserConfig dataParserConfig) {

	}
	
	/**
	 * 生成文件头 
	 *  文件头格式固定：
	 *  	DEB  HERMES  PSA       FE1172220110124003525
	 * @throws IOException 
	 */
	@Override
	public void fileBefore(OutputStreamWriter writer) {
			try {
				writer.write("("+DateUtil.dateToStringYMDHms(new Date())+" ATHENA-MAS INTERFACE"+interfaceId+" BEGIN==>)");
				writer.write("\n");
			} catch (IOException e) {
	            logger.error("接口" + interfaceId + "IO输出异常", e);
	            throw new ServiceException("接口" + interfaceId + "IO输出异常", e);
			}
	}
	
	/**
	 * 记录文件日志
	 * @author Hezg
	 * @date 2013-2-4
	 * @param wenjmc 文件名称
	 * @param file_satus 运行状态        1 成功(已执行)  -1 失败(已执行)
	 * @return 
	 */
	@Override
	protected void file_info(String wenjmc,boolean file_satus) {
	}
}
