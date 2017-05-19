package com.athena.component.input;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.util.exception.ServiceException;


/**
 * 2650
 * SPPV 6400点  整车过点信息
 * @author 贺志国
 * @E-mail zghe@isoftstone.com
 * @version v1.0
 * @date 2013-12-9
 */
public class Sppv6400DBDateWriter extends TxtInputDBSerivce {
	 private String datasourceId1 = "";
	    private String datasourceId2 = "";   
		protected static Logger logger = Logger.getLogger(Sppv6400DBDateWriter.class);	//定义日志方法

		public Sppv6400DBDateWriter(DataParserConfig dataParserConfig) {
			super(dataParserConfig);
			datasourceId1=dataParserConfig.getReaderConfig().getDatasourceId();
			datasourceId2=dataParserConfig.getWriterConfig().getDatasourceId();
		}
		
		/**
		 * 执行前将sppv.ATHENA001表中的车型MONVEH字段值更新为'S' 
		 * 2013-12-9
		 */
		@Override
		public void before(){
			try{
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId1)
				.execute("inPutzxc.updateATHENA001OfStateJsbsToJ");
			}catch(RuntimeException e){
				logger.error("sppv DatasourceId="+datasourceId1+"@线程--接口" + interfaceId +"SQL异常,before更新sppv.ATHENA001表STATE状态F为J时报错"+e.getMessage());
				throw new ServiceException("sppv DatasourceId="+datasourceId1+"@线程--接口" + interfaceId +"SQL异常,before更新sppv.ATHENA001表STATE状态F为J时报错"+e.getMessage());
			}
		}
		
		
		/**
		 * 1、接口处理完成后更新SPPV ATHENA001表的状态为'T'(已处理状态)
		 * 2、更新生产线为UL5L1形式
		 * 2013-12-9
		 */
		@SuppressWarnings("unchecked")
		@Override
		public void after() {
			try{
				//1、执行后将sppv.ATHENA001表中的车型MONVEH字段值更新为'E' 
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId1)
				.execute("inPutzxc.updateAthena001OfState");
				/*//2、ck_zhengcgd表中的shengxc值将产线更新为五位
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId2)
				.execute("inPutzxc.updateAthenaOfChanx");*/
				//3、归集表中的产线，最大序号，以及过点时间
				List<Map<String,String>> listShengcx = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId2).
				select("inPutzxc.queryGroupbyShengcxOfZhengcgd");
				//循环产线，根据产线进行6000点序号的更新
				for(Map<String,String> cxmap : listShengcx){
					//根据产线查出6000点的，并且xuh为空的数据，按过点时间排序（顺序）
					List<Map<String,String>> listZhengcxh = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId2).
            		select("inPutzxc.queryGroupbyXuhOfZhengcgd6000",cxmap);
					//取产线最大整车序号                                                  
					int zcxh =  Integer.parseInt(cxmap.get("ZHENGCXH").toString());	
					for(Map<String,String> xhmap : listZhengcxh){
						//根据产线和最大整车序号查询DDBH_PCJHLSH_CS表中最小的流水号
						String liush = (String) dataParserConfig.getBaseDao().getSdcDataSource(datasourceId2).
						selectObject("inPutzxc.queryLiushofPcjhlsh6000",cxmap);
						if (StringUtils.isEmpty(liush)){
						    ++zcxh;
						}else{
							zcxh = Integer.parseInt(liush);
						} 
						xhmap.put("sZhengcxh", String.valueOf(zcxh));
						xhmap.put("editor", interfaceId);
						//更新6000点的整车序号
        				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId2).execute("inPutzxc.updateZhengcxhOfZhengcgd",xhmap);
					}
				}
				
				
			}catch(RuntimeException e){
				logger.error("线程--接口" + interfaceId +"更新ATHENA001和ck_zhengcgd表时报错"+e.getMessage());
				throw new ServiceException("线程--接口" + interfaceId +"更新ATHENA001和ck_zhengcgd表时报错"+e.getMessage());
			}
		}
}
