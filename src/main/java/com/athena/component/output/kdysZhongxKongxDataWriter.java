package com.athena.component.output;

import java.io.IOException;
import java.io.OutputStreamWriter;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.config.ExchangerConfig;
import com.athena.component.exchange.db.DBOutputTxtSerivce;
import com.athena.util.exception.ServiceException;
/**
 * KDYS重箱空箱申报
 * @author hzg
 * @vesion 1.0
 * @date 2013-10-23
 */

public class kdysZhongxKongxDataWriter extends DBOutputTxtSerivce {
	public kdysZhongxKongxDataWriter(DataParserConfig dataParserConfig) {
		
	}
	
	/**
	 * 生成文件头 
	 *  文件头格式固定：
	 *  	COSCO
	 * @throws IOException 
	 */
	@Override
	public void fileBefore(OutputStreamWriter writer) {
			try {
				writer.write("COSCO");
				writer.write("\n");
			} catch (IOException e) {
	            logger.error("接口" + interfaceId + "IO输出异常", e);
	            throw new ServiceException("接口" + interfaceId + "IO输出异常", e);
			}
	}
	
	/**
	 * 生成文件尾
	 *  文件尾格式固定：
	 *      END 记录数
	 * @throws IOException 
	 */
	public void fileAfter(ExchangerConfig write, ExchangerConfig read,OutputStreamWriter out){
		try {
			out.write("END"+getTotal());
			out.write("\n");
		} catch (IOException e) {
            logger.error("接口" + interfaceId + "IO输出异常", e);
            throw new ServiceException("接口" + interfaceId + "IO输出异常", e);
		}
	}
	
}
