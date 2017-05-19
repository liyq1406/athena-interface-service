package com.athena.component.input;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.util.exception.ServiceException;


/**
 * 2040
 * 发货通知接口输入类
 * @author yz
 */
public class DFPVFhtz extends TxtInputDBSerivce{
	private String datasourceId = "";
	
	
	public DFPVFhtz(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
		datasourceId = dataParserConfig.getWriterConfig().getDatasourceId();
	}

	 /**
	 * 解析之前清空表数据
	 */
	@Override
	public void before() {
        try {
        	//EFI发货通知-发运主体信息表是否存在处理状态为1的数据
        	String jfdhTotal = (String)dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.selectObject("inPutzxc.queryJfdhTotal");
        	int i = Integer.parseInt(String.valueOf(strNull(jfdhTotal)));
			if(i>0){
				//清空EFI发货通知-同步零件信息表
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzxc.efiTbljxxDelete");
				//清空EFI发货通知-包装对应信息表
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzxc.efiBzdyxxDelete");
				//清空EFI发货通知-发运明细表
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzxc.efiFymxDelete");
	        	//清空EFI发货通知-发运主体信息表
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzxc.efiFyztxxDelete");
			}
		} catch (RuntimeException e) {
			logger.error("线程--接口" + dataParserConfig.getId() +"清除EFI发货通知相关表报错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"清除EFI发货通知相关表报错"+e.getMessage());
		}
	}
	
	
	/**
	 * 不用读取文件
	 */
	@Override
	public boolean read(DataParserConfig dataParserConfig){		
		interfaceId = dataParserConfig.getId();	
		logger.info("接口" + interfaceId + "开始输入");
		before();
		after();
		logger.info("接口" + interfaceId + "结束输入");
		return true;
	}

	/**
	 * 空串处理
	 * @param obj对象
	 * @return 处理后字符串
	 */
	private String strNull(Object obj) {// 对象为空返回空串,不为空toString
		return obj == null ? "" : obj.toString();
	}
	
	/**
	 * 查询数据插入到货通知单
	 * @param daohtzdList
	 * @throws SQLException
	 */
	public  void insertDaohtzd(List<Map<String,Object>> daohtzdList){
		try {
			for(Map<String,Object> map:daohtzdList){
				//查询ck_daohtzd_dfpv表是否已经存在此数据
				int i = (Integer) dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.selectObject("inPutzxc.queryck_daohtzd_dfpv",map);
				//表示表中不存在此数据
				if(i<1){
					String daoh_seqno = (String)dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
					.selectObject("inPutzxc.queryDaoh_seqno");
					String usercenter = (String) map.get("USERCENTER");
					String tch = (String) map.get("TCH");
					String chengysdm = (String) map.get("CHENGYSDM");
					String chengysmc = (String) map.get("CHENGYSMC");
					String yujddsj = (String) map.get("YUJDDSJ");
					String fayzmzdw = (String) map.get("FAYZMZDW");
					String psasj = (String) map.get("PASSJ");
					String changh = (String) map.get("CHANGH");
					String cangkbh = (String) map.get("CANGKBH");
					//uth为用户中心后一位+0+序列
					String uth = usercenter.substring(1).trim()+daoh_seqno;
					map.put("UTH", uth);
				    //原始uth为uth
					map.put("YUANSUTH",uth);
					//这些字段进行为空替换
					map.put("TCH", strNull(tch));
					map.put("CREATOR", interfaceId);
					map.put("EDITOR", interfaceId);
					map.put("CHENGYSDM", strNull(chengysdm));
					map.put("CHENGYSMC", strNull(chengysmc));
					map.put("YUJDDSJ", strNull(yujddsj));
					map.put("FAYZMZDW", strNull(fayzmzdw));
					map.put("PASSJ", strNull(psasj));
					map.put("CHANGH", strNull(changh));
					map.put("CANGKBH", strNull(cangkbh));
					//插入ck_daohtzd_dfpv表
					dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
					.execute("inPutzxc.insertDaohtzd_dfpv", map);			
	        	}
			}
		} catch (RuntimeException e) {
			throw new ServiceException("线程--接口" + interfaceId +"插入ck_daohtz_dfpv出错"+e.getMessage());
		}
	}
	
	/**
	 * 查询数据插入到uabq表
	 * @param uabqList
	 * @throws SQLException
	 */
	public void insertUabq(List<Map<String,Object>> uabqList){
		try {
			List<String> dataList = new ArrayList<String>();
			Map<String,String> params = new HashMap<String,String>();
			for(Map<String,Object> map:uabqList){
				//查询uabq表是否已经存在此数据
				int i = (Integer) dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.selectObject("inPutzxc.queryck_uabq_dfpv",map);
				//没有此数据才插入uabq表
				if(i<1){
					String usercenter = (String) map.get("USERCENTER");
					String gongysdm = strNull(map.get("GONGYSDM"));
					String blh = (String) map.get("BLH");
					String lingjbh = strNull((String) map.get("LINGJBH"));
					String pich = strNull((String) map.get("PICH"));
					//用来存放供应商+交付单号 和 零件+供应商+批次
					String ulh = "";
					String elh = "";
					//用来存放ulh和elh
					//查询是否存在相同的供应商和交付单号,供应商交付单号相同生成一个ulh
					if(!dataList.contains(gongysdm+blh)){
						String ul_seqno = (String)dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
						.selectObject("inPutzxc.queryul_seqno");
						ulh = usercenter.substring(1).trim()+ul_seqno;
						params.put(gongysdm+blh, ulh);
						dataList.add(gongysdm+blh);
					}else{
						ulh = params.get(gongysdm+blh);
					}
					//查询是否存在相同的供应商、零件号和批次号,供应商零件号和批次号相同生成一个el
					if(!dataList.contains(gongysdm+lingjbh+pich)){
						String el_seqno = (String)dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
						.selectObject("inPutzxc.queryel_seqno");
						elh = el_seqno;
						params.put(gongysdm+lingjbh+pich, elh);
						dataList.add(gongysdm+lingjbh+pich);
					}else{
						elh = params.get(gongysdm+lingjbh+pich);
					}
					int ucgs = 0;
					String lingjsl =  strNull(map.get("LINGJSL"));
					String uchl =  map.get("UCHL")==null?"0":map.get("UCHL").toString();	
					//如果uc容量为0或者空uc个数就为0;zx
					if(strNull(uchl).equals("")||Double.parseDouble(uchl)==0){
						ucgs = 0;
					}else{
						ucgs = (int)(Double.parseDouble(lingjsl)/Double.parseDouble(uchl));
					}
					String gongysmc = (String) map.get("GONGYSMC");
					String gongyslx = (String) map.get("GONGYSLX");
					String uaxh = (String) map.get("UAXH");
					String uarl = strNull(map.get("UARL"));
					String ucxh = (String) map.get("UCXH");
					String lingjmc = (String) map.get("LINGJMC");
					String cangkbh = (String) map.get("CANGKBH");
					String zickbh = (String) map.get("ZICKBH");
					String danw = (String) map.get("DANW");
					String zhuangtsx = (String) map.get("ZHUANGTSX");
					String chanx = (String) map.get("CHANX");
					String jingz = strNull(map.get("JINGZ"));
					String maoz =  strNull(map.get("MAOZ"));
					String yanssl = map.get("YANSSL").toString();
					String yaohlh = (String) map.get("YAOHLH");
					String dingdh = (String) map.get("DINGDH");
					String quancbqh = (String) map.get("QUANCBQH");
					String xiaohd = (String) map.get("XIAOHD");
					String dinghckkw = (String) map.get("DINGHCKKW");
					String xianbck = (String) map.get("XIANBCK");
					String xianbckkw = (String) map.get("XIANBCKKW");
					String daybs = (String) map.get("DAYBS");
					String shiftjkw = (String) map.get("SHIFTJKW");
					String xiaohcbh = (String) map.get("XIAOHCBH");
					String tangc =  strNull(map.get("TANGC"));
					String shifht = (String) map.get("SHIFHT");
					String xiangd = (String) map.get("XIANGD");
					String gongzr = (String) map.get("GONGZR");	
					String changd = (String) map.get("CHANGD");
					if(StringUtils.isEmpty(changd)){
						changd = "1";
					}
					String kuand = (String) map.get("KUAND");
					if(StringUtils.isEmpty(kuand)){
						kuand = "1";
					}
					String gaod = (String) map.get("GAOD");
					if(StringUtils.isEmpty(gaod)){
						gaod = "1";
					}
					map.put("GONGYSDM", gongysdm);
					map.put("LINGJBH", lingjbh);
					map.put("PICH", pich);
					map.put("UCHL", strNull(uchl));
					map.put("UCGS", strNull(ucgs));
					map.put("LINGJSL", strNull(lingjsl));
					map.put("UCGS", ucgs);
					map.put("ULH", ulh);
					map.put("ELH", elh);
					map.put("GONGYSMC", strNull(gongysmc));
					map.put("GONGYSLX", strNull(gongyslx));
					map.put("UAXH", strNull(uaxh));
					map.put("UARL", strNull(uarl));
					map.put("UCXH", strNull(ucxh));
					map.put("LINGJMC", strNull(lingjmc));
					map.put("CANGKBH", strNull(cangkbh));
					map.put("ZICKBH", strNull(zickbh));
					map.put("DANW", strNull(danw));
					map.put("ZHUANGTSX", strNull(zhuangtsx));
					map.put("CHANX", strNull(chanx));
					map.put("JINGZ", strNull(jingz));
					map.put("MAOZ", strNull(maoz));
					map.put("YANSSL", strNull(yanssl));
					map.put("YAOHLH", strNull(yaohlh));
					map.put("DINGDH", strNull(dingdh));
					map.put("QUANCBQH", strNull(quancbqh));
					map.put("XIAOHD", strNull(xiaohd));
					map.put("DINGHCKKW", strNull(dinghckkw));
					map.put("XIANBCK", strNull(xianbck));
					map.put("XIANBCKKW", strNull(xianbckkw));
					map.put("DAYBS", strNull(daybs));
					map.put("SHIFTJKW", strNull(shiftjkw));
					map.put("XIAOHCBH", strNull(xiaohcbh));
					map.put("TANGC", strNull(tangc));
					map.put("SHIFHT", strNull(shifht));
					map.put("XIANGD", strNull(xiangd));
					map.put("GONGZR", strNull(gongzr));
					map.put("CAOZY", interfaceId);
					map.put("CREATOR", interfaceId);
					map.put("EDITOR", interfaceId);
					map.put("CHANGD", changd);
					map.put("KUAND", kuand);
					map.put("GAOD", gaod);
					//插入ck_uabq_dfpv表
					dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
					.execute("inPutzxc.insertUabq_dfpv", map);
	        	}
			}
		}
		catch (RuntimeException e) {
			throw new ServiceException("线程--接口" + interfaceId +"插入ck_uabq_dfpv出错"+e.getMessage());
		}
		
	}
	
	/**
	 * 接口运行完处理方法
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void after() {
		try {
			//更新in_efi_fhtz_fyztxx clzt=5的为6
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.execute("inPutzxc.updatein_efi_fhtz_fyztxx_6");
			//查询需要插入ck_daohtzd_dfpv表的数据
			List<Map<String,Object>> daohtzdList = dataParserConfig.getBaseDao()
			.getSdcDataSource(datasourceId).select("inPutzxc.queryDaohtzd_dfpv_6");
			//判断是否为空，为空说明没有数据需要插入Daohtzd_dfpv表
			if(!daohtzdList.isEmpty()){
				insertDaohtzd(daohtzdList);
				//查询需要插入ck_uabq_dfpv表的数据
				List<Map<String,Object>> uabqList = dataParserConfig.getBaseDao()
				.getSdcDataSource(datasourceId).select("inPutzxc.queryUabq_dfpv_6");
				insertUabq(uabqList);
				//更新in_efi_fhtz_fyztxx 传到DFPV的clzt为1
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutzxc.updatein_efi_fhtz_fyztxx_dfpv_1");
				//更新in_efi_fhtz_fyztxx 其它的clzt为0
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutzxc.updatein_efi_fhtz_fyztxx_0");
			}else{
				//更新in_efi_fhtz_fyztxx clzt=6为0
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutzxc.updatein_efi_fhtz_fyztxx_0");
			}
		}catch (RuntimeException e) {
			logger.error(e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"出错"+e.getMessage());
		} 
	}
	
	
}
