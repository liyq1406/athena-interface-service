package com.athena.component.output;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.db.DBOutputTxtSerivce;
import com.athena.util.exception.ServiceException;
/**
 * 仓库入库明细
 * @author chenlei
 * @vesion 1.0
 * @date 2012-4-18
 * 
 * lastModify  By 王冲, 2012-08-31 11:18  内容：注掉全部代码
 */

public class RkmxDataWriter extends DBOutputTxtSerivce {
	
	/**
	 * 将要输出的数据写到中间表in_cklsz
	 * @param dataParserConfig
	 */
	public RkmxDataWriter(DataParserConfig dataParserConfig) {
		
	}
	
	/**
	 * 执行前将业务表数据写到中间表In_cklsz
	 * @author hzg
	 * @date 2015.12.12
	 */
	@Override
	public void before(){
		/** 清除中间表*/
		dataParserConfig.getBaseDao().getSdcDataSource(sourceId).execute("outPut.deleteInCklsz");
		
		logger.info("仓库流水账写中间表in_cklsz开始 "+dateToStringYMDHms());
		try{
			/** 入库 A01,A01S,A03 */
			dataParserConfig.getBaseDao().getSdcDataSource(sourceId).execute("outPut.insertKuclszQuery");
			/** 入库返修 A08,A08S,A10 */
			dataParserConfig.getBaseDao().getSdcDataSource(sourceId).execute("outPut.insertCangklszQuery");
			/** 纠纷单A25,A25S  */
			dataParserConfig.getBaseDao().getSdcDataSource(sourceId).execute("outPut.insertCangklszjfQuery");
			/** 备货出库单A35 */
			dataParserConfig.getBaseDao().getSdcDataSource(sourceId).execute("outPut.insertCangklszbhdQuery");
			/**备货出库单A36 */
			dataParserConfig.getBaseDao().getSdcDataSource(sourceId).execute("outPut.insertCangklszbhdtsQuery");
			/** 正常退库 A38 */
			dataParserConfig.getBaseDao().getSdcDataSource(sourceId).execute("outPut.insertCangklszzctkQuery");
			/** 正常退库 A39 */
			dataParserConfig.getBaseDao().getSdcDataSource(sourceId).execute("outPut.insertCangklszzctktnQuery");
			/** 盘点差异A53  */
			dataParserConfig.getBaseDao().getSdcDataSource(sourceId).execute("outPut.insertCangklszpdcyQuery");
			/** 移库 A70 */
			dataParserConfig.getBaseDao().getSdcDataSource(sourceId).execute("outPut.insertCangklszyikQuery");
			/** 报废 A11 A11S  */
			dataParserConfig.getBaseDao().getSdcDataSource(sourceId).execute("outPut.insertBaofeiQuery");
			logger.info("仓库流水账写中间表in_cklsz结束 "+dateToStringYMDHms());
		}catch(RuntimeException e)
		{
			logger.error("线程--接口" + interfaceId +"读取仓库流水账写中间表in_cklsz出错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"读取仓库流水账写中间表in_cklsz出错"+e.getMessage());
		}
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
				writer.write("DEB  ATHENA  ath1osap03FE11722"+dateToStringYMDHms());
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
	
	/**
	 * 将日期转换为yyyy-MM-dd HH:mm:ss字符串
	 * @date 2012-1-18 
	 * @author hzg
	 * @param date
	 * @return String 日期字符串
	 */
	public static String dateToStringYMDHms(){
		DateFormat YMDHmsFormat = new SimpleDateFormat("yyyyMMddHHmmss",Locale.CHINA);
		return YMDHmsFormat.format(new Date());
	}
	
}
