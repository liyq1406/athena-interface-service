package com.athena.component.input;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.util.exception.ServiceException;


/**
 * 2950
 * SPPV焊装过点申报信息
 * @author 吕婵
 * @date 2015-12-22
 */
public class SppvhzgdsbxxDBDateWriter extends TxtInputDBSerivce {
	 private String datasourceId1 = "";
	    private String datasourceId2 = "";   
		protected static Logger logger = Logger.getLogger(SppvhzgdsbxxDBDateWriter.class);	//定义日志方法

		public SppvhzgdsbxxDBDateWriter(DataParserConfig dataParserConfig) {
			super(dataParserConfig);
			datasourceId1=dataParserConfig.getReaderConfig().getDatasourceId();
			datasourceId2=dataParserConfig.getWriterConfig().getDatasourceId();
		}
		
		/**
		 * 执行前将sppv.ATHENA006表中的车型MONVEH字段值不为'E'的更新为'S' 
		 * 2015-12-22
		 */
		@Override
		public void before(){
			try{
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId1)
				.execute("inPutzxc.updateATHENA006OfStateJsbsToJ");
			}catch(RuntimeException e){
				logger.error("sppv DatasourceId="+datasourceId1+"@线程--接口" + interfaceId +"SQL异常,before更新sppv.ATHENA006表STATE状态F为J时报错"+e.getMessage());
				throw new ServiceException("sppv DatasourceId="+datasourceId1+"@线程--接口" + interfaceId +"SQL异常,before更新sppv.ATHENA006表STATE状态F为J时报错"+e.getMessage());
			}
		}
		
		
		/**
		 * 1、接口处理完成后更新SPPV ATHENA006表的状态为'T'(已处理状态)
		 * 2、更新生产线为UL2L1形式
		 * 2015-12-22
		 */
		@SuppressWarnings("unchecked")
		@Override
		public void after() {
			try{
				//1、执行后将sppv.ATHENA006表中的车型MONVEH字段值更新为'E' 
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId1)
				.execute("inPutzxc.updateAthena006OfState");
				/*//2、ck_zhengcgd表中的shengxc值将产线更新为五位
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId2)
				.execute("inPutzxc.updateAthena006OfChanx");*/
				//3、归集表中的物理点，产线，最大整车序号
				List<Map<String,String>> listShengcx = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId2).
				select("inPutzxc.queryGroupbyShengcx");
				//循环产线，根据产线进行过点时间序号的更新
				for(Map<String,String> cxmap : listShengcx){
					//根据产线查出每个物理点整车序号为空的数据，按过点时间排序（顺序）
					List<Map<String,String>> listZhengcxh = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId2).
            		select("inPutzxc.queryGroupbyXuhOfZhengcgd",cxmap);
					//取产线最大整车序号                                                  
					int zcxh =  Integer.parseInt(cxmap.get("ZHENGCXH").toString());	
					for(Map<String,String> xhmap : listZhengcxh){
						//根据产线和最大整车序号查询DDBH_PCJHLSH_CS表中最小的流水号
						String liush = (String) dataParserConfig.getBaseDao().getSdcDataSource(datasourceId2).
						selectObject("inPutzxc.queryLiushofPcjhlsh",cxmap);
						if (StringUtils.isEmpty(liush)){
							++zcxh;
						}else{
							zcxh = Integer.parseInt(liush);
						} 
						xhmap.put("sZhengcxh", String.valueOf(zcxh));
						xhmap.put("editor", interfaceId);
						//更新整车序号
        				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId2).execute("inPutzxc.updateZhengcxh",xhmap);
					}
				}
				
				
			}catch(RuntimeException e){
				logger.error("线程--接口" + interfaceId +"更新ATHENA006和ck_zhengcgd表时报错"+e.getMessage());
				throw new ServiceException("线程--接口" + interfaceId +"更新ATHENA006和ck_zhengcgd表时报错"+e.getMessage());
			}
		}
}
