package com.athena.component.output;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.athena.component.DateTimeUtil;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.config.ExchangerConfig;
import com.athena.component.exchange.db.DBOutputTxtSerivce;
import com.athena.component.service.bean.ZongccfBean;
import com.athena.util.date.DateUtil;
import com.athena.util.exception.ServiceException;

/**
 * 3280 总成计划（输出）
 * @author lc
 * @date 2015-12-29
 */
public class ZongccfqqTxtWriter extends DBOutputTxtSerivce{
	private String datasourceId = "";   
	protected static Logger logger = Logger.getLogger(ZongccfqqTxtWriter.class);	//定义日志方法

	public ZongccfqqTxtWriter(DataParserConfig dataParserConfig) {
		datasourceId=dataParserConfig.getReaderConfig().getDatasourceId();
	}

	@Override
	public void fileBefore(OutputStreamWriter writer) {
		String BeginTime=DateTimeUtil.getDateTimeStr("yyyy-MM-dd HH:mm:ss");
		try {
			writer.write("("+BeginTime+" ATHENA—PDS INTERFACE05 BEGIN==>"+")");
			writer.write("\r\n");
		} catch (IOException e) {
			logger.error("接口" + interfaceId + "IO输出异常", e);
			throw new ServiceException("接口" + interfaceId + "IO输出异常", e);
		}

	}

	@Override
	public void fileAfter(ExchangerConfig write, ExchangerConfig read,OutputStreamWriter out) {
		String EndTime=DateTimeUtil.getDateTimeStr("yyyy-MM-dd HH:mm:ss");
		try {
			out.write("("+EndTime+" ATHENA—PDS INTERFACE05 RECORDS="+this.getTotal()+" END<=="+")");
		} catch (IOException e) {
			logger.error("接口" + interfaceId + "IO输出异常", e);
			throw new ServiceException("接口" + interfaceId + "IO输出异常", e);
		}
	}

	/**
	 * 1.运行前清空in_zongccfqq_tmp中间表
	 * 2.将数据插入到中间表in_zongccfqq_tmp
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void before() {
		//运行前清空in_zongccfqq_tmp中间表
		logger.info("线程--接口" + interfaceId + "开始清空中间表in_zongccfqq_tmp，并将数据插入到in_zongccfqq_tmp中间表");	
		dataParserConfig.getBaseDao().getSdcDataSource(sourceId).execute("outPut.truncateddbhzongccfqq");
		int num1 = dataParserConfig.getBaseDao().getSdcDataSource(sourceId).execute("outPut.insertddbhzongccfqq");
		logger.info("线程--接口" + interfaceId + "将数据插入到in_zongccfqq_tmp中间表结束，插入"+num1+"条");
		try{
			//查询in_clddxx表中最大预计进装总量时间
			String maxYjsj = (String)dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).selectObject("outPut.querymaxyjjzzlsj");
			if(StringUtils.isNotEmpty(maxYjsj)){
				List<ZongccfBean> list = new ArrayList<ZongccfBean>();
				//归集表中的制造车间，产品号
				List<Map<String,String>> listzhizcjchanph = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).
				select("outPut.queryGroupbyzhizcjchanph");			
				//循环，根据制造车间，产品号进行ECOM日期的更新
				for(Map<String,String> cxmap : listzhizcjchanph){
					ZongccfBean zccfBean = null;	
					String curDate = DateUtil.getCurrentDate();
					int d = DateUtil.getDaysOfDateSubtract(curDate, maxYjsj);
					if(d<=0){
						list.add(setZccfBean(cxmap,curDate));
					}
					for(int i=0;i<d;i++){
						if(i<35){
							zccfBean = new ZongccfBean();
							String zhankrq = DateUtil.dateAddDays(curDate,i);
							zccfBean = setZccfBean(cxmap,zhankrq);
							list.add(zccfBean);
						}
					}
				}
				logger.info("线程--接口" + interfaceId + "将数据插入到in_zongccfqq_tmp中间表");
				int num = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).executeBatch("outPut.insertddbhzongccfqqfzx",list);
				logger.info("线程--接口" + interfaceId + "将数据插入到in_zongccfqq_tmp中间表结束，插入"+num+"条");
			}
		}catch(RuntimeException e){
			logger.error("线程--接口" + interfaceId + "将数据插入到in_zongccfqq_tmp中间表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId + "将数据插入到in_zongccfqq_tmp中间表时报错"+e.getMessage());
		}		
	}

	private ZongccfBean setZccfBean(Map<String,String> cxmap,String ecomrq){
		ZongccfBean zccfBean = new ZongccfBean();
		zccfBean.setZhizcj(cxmap.get("ZHIZCJ").toString());
		zccfBean.setChanph(cxmap.get("CHANPH").toString());
		zccfBean.setJisbs("1");
		zccfBean.setEcomrq(ecomrq);
		zccfBean.setShul("00000001");
		zccfBean.setZhengfbj("+");
		zccfBean.setYingybs("ATH");
		return zccfBean;
	}
}
