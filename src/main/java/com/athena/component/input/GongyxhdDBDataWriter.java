package com.athena.component.input;


import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.util.exception.ServiceException;

/**
 * 2080 工艺消耗点
 * @author hzg
 *
 */
public class GongyxhdDBDataWriter extends TxtInputDBSerivce {
	private List<String> listData = new ArrayList<String>();
	private  Date date = new Date();
	private String datasourceId = "";
	public GongyxhdDBDataWriter(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
		datasourceId = dataParserConfig.getWriterConfig().getDatasourceId();
	}
	
	/**
	 * 行解析之后处理方法
	 * @param record 行数据集合
	 */
	@Override
	public boolean afterRecord(Record record) {
		record.put("CREATOR", interfaceId);
		record.put("CREATE_TIME", date);
		record.put("EDITOR", interfaceId);
		record.put("EDIT_TIME", date);
		
		String GONGYXHD = record.getString("GONGYXHD").substring(0,3);
		String SHENGCXBH = record.getString("SHENGCXBH").substring(0,2);
		String gyxhd = GONGYXHD + SHENGCXBH;
		if(!listData.contains(gyxhd)){
			listData.add(gyxhd);
		}
		return true;
	}
	
	/**
	 * 处理完之后调用
	 */
	@Override
	public void after(){
		addTable();
		try{
			Map<String,String> param = new HashMap<String,String>();
			param.put("EDITOR", interfaceId);
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzxc.updateGongyxhdRongqc",param);
		}catch(RuntimeException e){
			logger.error("线程--接口" + interfaceId +"执行update工艺消耗点表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"执行update工艺消耗点表时报错"+e.getMessage());
		}	
	}
	
	/**
	 * 添容器区表
	 */
	private void insertRQC(String usercenter,String rongqcbh) {
		try{
			//写数据库
			Map<String,String> param = new HashMap<String,String>();
			param.put("usercenter", strNull(usercenter));
			param.put("rongqcbh", strNull(rongqcbh));
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzxc.insertRongqc",param);
		}catch(RuntimeException e){
			logger.error("线程--接口" + interfaceId +"执行insert容器区表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"执行insert容器区表时报错"+e.getMessage());
		}			
	}
	

	//删除接口外部要货令表表数据
	//使用接口表 连接
	private void addTable() {	
		if(!listData.isEmpty()){
			for(int i=0;i<listData.size();i++){
				String rongqcbh = listData.get(i).toString().substring(0,3);
				String usercenter = strNull(listData.get(i).toString().substring(3,5));
				if(!QueryTable(usercenter,rongqcbh)){
					insertRQC(usercenter,rongqcbh);
				}
				
			}
		}
	}
	
	/**查找主键为条件的数据是否已经存在
	 * @param usercenter 用户中心
	 * @param rongqcbh 容器场编号
	 * @return boolean
	 */
	public boolean QueryTable(String usercenter,String rongqcbh){
		boolean flag=false;
		Map<String,String> param = new HashMap<String,String>();
		param.put("usercenter", strNull(usercenter));
		param.put("rongqcbh", strNull(rongqcbh));
		try{
			String c = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).selectObject("inPutzxc.queryRongqcCount",param).toString();
			int count = Integer.valueOf(c);
			if(count>0){
				flag = true;
			}
		}catch(RuntimeException e){
			logger.error("线程--接口" + interfaceId +"执行select容器区表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"执行select容器区表时报错"+e.getMessage());
		}
		return flag;
	}
	
	/**
	 * 空串处理
	 * @param obj
	 *  对象
	 * @return 处理后字符串
	 * @author GJ
	 * @date 2011-10-26
	 */
	private String strNull(Object obj) {// 对象为空返回空串,不为空toString
		return obj == null || obj.equals("null")? "" : obj.toString();
	}

}
