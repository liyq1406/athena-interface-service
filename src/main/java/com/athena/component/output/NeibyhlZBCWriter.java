package com.athena.component.output;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.db.DBOutputTxtSerivce;

public class NeibyhlZBCWriter extends  DBOutputTxtSerivce{
	public NeibyhlZBCWriter(DataParserConfig dataParserConfig) {
	}

	/**
	 * 2570接口
	 * 运行前现删除ck_yaonbhl_zbc表中输入，然后将要输出的内部要货令数据插入到ck_yaonbhl_zbc表
	 */
	@Override
	public void before() {
		baseDao.getSdcDataSource(sourceId).execute("outPut.deleteNeibyhlZBCBegin");
		int num = baseDao.getSdcDataSource(sourceId).execute("outPut.insertNeibyhlZBCBegin");
		logger.info("线程--接口" + interfaceId +" 插入要货令条数为："+num);
	}

}
   