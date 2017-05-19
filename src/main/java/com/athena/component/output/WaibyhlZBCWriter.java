package com.athena.component.output;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.db.DBOutputTxtSerivce;

public class WaibyhlZBCWriter extends  DBOutputTxtSerivce{
	public WaibyhlZBCWriter(DataParserConfig dataParserConfig) {
	}

	/**
	 * 2380接口
	 * 运行前现删除ck_yaohl_zbc表中输入，然后将要输出的要货令数据插入到ck_yaohl_zbc表
	 */
	@Override
	public void before() {
		baseDao.getSdcDataSource(sourceId).execute("outPut.deleteWaibyhlZBCBegin");
		int num = baseDao.getSdcDataSource(sourceId).execute("outPut.insertWaibyhlZBCBegin");
		logger.info("线程--接口" + interfaceId +" 插入要货令条数为："+num);
	}
}
   