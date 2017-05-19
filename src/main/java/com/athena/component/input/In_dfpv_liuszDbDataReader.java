package com.athena.component.input;


import java.util.Date;
import java.util.List;
import java.util.Map;

import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.util.exception.ServiceException;


/**
 * 3550 DFPV仓库流水账
 * 2970 DFPV仓库原始流水账
 * @date 2015-12-25
 * @author yz
 */
public class In_dfpv_liuszDbDataReader extends TxtInputDBSerivce{
	private Date date = new Date();
	private String datasourceId = "";

	public In_dfpv_liuszDbDataReader(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
		datasourceId = dataParserConfig.getWriterConfig().getDatasourceId();
	}
	

	/**
	 * 行解析之后处理方法
	 * 2970
	 * 财务日期加-变成YYYY-MM-DD格式，但其实还是char型
	 * flag 默认为0
	 */
	@Override
	public boolean afterRecord(Record record) {
		if(interfaceId.equals("2970")){
			String caiwrq = record.getString("CAIWRQ");			
			caiwrq = caiwrq.substring(0,4)+"-"+caiwrq.substring(4,6)+"-"+caiwrq.substring(6);
			record.put("CAIWRQ", caiwrq);
			//原始流水账用，其他的表不用
			record.put("FLAG", 0);
		}
		record.put("EDITOR",interfaceId);
		record.put("EDIT_TIME",date);
		record.put("CREATOR",interfaceId);
		record.put("CREATE_TIME",date);
		return true;
	}



	/**
	 * 2970
	 * 接口运行后处理方法
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void after() {
		if(interfaceId.equals("2970")){
			try{
				// 查询得到单位换算后的单位和数量
				List<Map<String, Object>> lingjdwList = dataParserConfig.getBaseDao()
				.getSdcDataSource(datasourceId).select("inPutzxc.queryshuldanw_dfpv");
				// 更新零件单位和数量
				for (Map<String, Object> map : lingjdwList) {
					dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
							.execute("inPutzxc.updateshuldanw_dfpv", map);
				}
			}catch(RuntimeException e){
				logger.error("线程--接口" + interfaceId +"单位换算时报错"+e.getMessage());
				throw new ServiceException("线程--接口" + interfaceId +"单位换算时报错"+e.getMessage());
			}
			try{
				//操作码为B30的更新零件数量
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutzxc.updatein_dfpv_yuanslsz_B30");
			}catch(RuntimeException e){
				logger.error("线程--接口" + interfaceId +"更新B30零件数量时报错"+e.getMessage());
				throw new ServiceException("线程--接口" + interfaceId +"更新B30零件数量时报错"+e.getMessage());
			}
			try{
				//操作码为B36的用lingjbh关联xqjs_diaobmx表获取仓库子仓库
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutzxc.updatein_dfpv_yuanslsz_B36");
			}catch(RuntimeException e){
				logger.error("线程--接口" + interfaceId +"更新B36仓库子仓库时报错"+e.getMessage());
				throw new ServiceException("线程--接口" + interfaceId +"更新B36子仓库时报错"+e.getMessage());
			}
			try{	
				//操作码不是B36的用lingjbh关联ckx_wullj取线边仓库作为仓库和仓库子仓库，取到的数据beiz改为'1'
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutzxc.updatein_dfpv_yuanslsz");
			}catch(RuntimeException e){
				logger.error("线程--接口" + interfaceId +"更新非B36仓库子仓库时报错"+e.getMessage());
				throw new ServiceException("线程--接口" + interfaceId +"更新非B36仓库子仓库时报错"+e.getMessage());
			}
			try{
				//操作码为B10的插入ck_yicsbcz表
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutzxc.insertck_yicsbcz");
			}catch(RuntimeException e){
				logger.error("线程--接口" + interfaceId +"插入ck_yicsbcz表时报错"+e.getMessage());
				throw new ServiceException("线程--接口" + interfaceId +"插入ck_yicsbcz表时报错"+e.getMessage());
			}
			try{				
				//将系统参数定义表里面zidlx = 'DFPVLSZ'且定义的操作码的或者原始流水表caozm='B36'的插入DFPV_SHENH
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutzxc.insertdfpv_shenh");
				//将插入的数据的flag更新为1
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutzxc.updatein_dfpv_yuanslsz_shenh");					
				//将flag='0'的数据插入IN_DFPV_LIUSZ
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutzxc.insertin_dfpv_liusz");
				//更新in_dfpv_yuanslsz 的flag='0'更新为'1'
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutzxc.updatein_dfpv_yuanslsz_liusz");	
			}catch(RuntimeException e){
				logger.error("线程--接口" + interfaceId +"插入时报错"+e.getMessage());
				throw new ServiceException("线程--接口" + interfaceId +"插入时报错"+e.getMessage());
			}
		}
		
	}
	
}
