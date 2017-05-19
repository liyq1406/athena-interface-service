package com.athena.component.output;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.db.DBOutputTxtSerivce;

/**
 * 处理返修单和退货单表 操作码为O8的结果集输出
 * 
 * @author chenlei
 * @vesion 1.0
 * @date 2012-4-19
 * lastModify  By 王冲, 2012-08-31 13:18  内容：注掉全部代码
 */
public class Rkmx08DataWriter extends DBOutputTxtSerivce {
	public Rkmx08DataWriter(DataParserConfig dataParserConfig) {

	}

	@Override
	public void moveOutFile(String encoding,String oldfileName,String outPutfileName) {
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
