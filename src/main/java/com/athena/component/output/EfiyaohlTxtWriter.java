package com.athena.component.output;
import java.io.IOException;
import java.io.OutputStreamWriter;

import com.athena.component.DateTimeUtil;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.config.ExchangerConfig;
import com.athena.component.exchange.db.DBOutputTxtSerivce;
import com.athena.util.exception.ServiceException;

public class EfiyaohlTxtWriter extends  DBOutputTxtSerivce{
	public EfiyaohlTxtWriter(DataParserConfig dataParserConfig) {
	}

	/**
	 * 运行前更新beiz3的状态为0
	 */
	@Override
	public void before() {
		baseDao.getSdcDataSource(sourceId).execute("outPut.updateefiyaohlbegin");
//		dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
//		.execute("outPut.updateefiyaohlbegin");
	}
    

	/**
	 * 运行前更新beiz3的状态为1
	 */
	@Override
	public void after() {
//		try {
			baseDao.getSdcDataSource(sourceId).execute(
					"outPut.updateefiyaohlEnd");
//		} catch (Exception e) {
//			// TODO: handle exception
//		}
	}

}
   