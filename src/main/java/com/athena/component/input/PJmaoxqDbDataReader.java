package com.athena.component.input;

import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.athena.component.DateTimeUtil;
import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.component.exchange.txt.WarmBusinessException;
import com.athena.component.exchange.utils.ConvertUtils;
import com.athena.util.exception.ServiceException;

public class PJmaoxqDbDataReader extends TxtInputDBSerivce {

	protected static Logger logger = Logger.getLogger(PJmaoxqDbDataReader.class);	//定义日志方法
	/**
	 * 需求版次流水号
	 */
	private static int liush = 1;
	private String datasourceId = null;
	Calendar calendar = Calendar.getInstance();
	Set<String> set = new HashSet<String>();
	private String biaoz = "";
	public PJmaoxqDbDataReader(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
		datasourceId = dataParserConfig.getWriterConfig().getDatasourceId();
	}

	
	/**
	 * 文件解析后处理前获取毛需求表流水号
	 */
	@Override
	public void before() {
		//清除in_maoxqmx表数据  hzg 2013-5-7
		try{
			if ("1061".equals(interfaceId)) {
				biaoz = "4";
			}else if ("1062".equals(interfaceId)) {
				biaoz = "5";
			}else if ("1063".equals(interfaceId)) {
				biaoz = "6";
			}else if ("1064".equals(interfaceId)) {
				biaoz = "7";
			}
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.execute("inPutzbc.inMaoxqmxDelete",biaoz);
		}catch(RuntimeException e){
			logger.error("线程--接口" + dataParserConfig.getId() +"清除in_maoxqmx表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"清除in_maoxqmx表时报错"+e.getMessage());
		}
		
		
		//根据需求来源和拆分日期查询毛需求版次表版次记录，如果没有则添加一条新纪录	public static final SimpleDateFormat yyyyMMddHHmmssSSS = new SimpleDateFormat("yyyyMMddHHmmssSSS");
		SimpleDateFormat yyyyMMdd = new SimpleDateFormat("yyyyMMdd"); 
		String liushString = ConvertUtils.strNull((String)dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.selectObject("inPutzbc.queryLiushOfMaoxq", yyyyMMdd.format(new Date())));
		if(liushString.equals("0")){
			liush = 1;
		}else{
			liush = Integer.parseInt(liushString);
			liush++;
		}
	}
	
	/**
	 * 行解析之后处理方法
	 * 文本中出现非法字符并且标识不为"1"时跳过解析
	 */
	@Override
	public boolean beforeRecord(String line, String fileName, int lineNum){
		if("".equals(line)){ 
			return false;
		}
		String biaos=line.toString().substring(75,76).trim();
		boolean isFlag = "1".equals(biaos)?true:false;
		return isFlag;
	}
	
	/**
	 * 行解析之后处理方法
	 * @param rowIndex 行标
	 * @param record 行数据集合
	 * @author GJ
	 * @update hzg 2012-10-15
	 */
	@Override
	public boolean afterRecord(Record record) {
		try { 
			boolean flag = true;
			logger.info("记录数：" + record.getLineNum());
				if(StringUtils.isNotEmpty(record.get("xuqrq").toString())){		
					//如果来源为JLV，处理JLV数据   update 2016.4.4 不对JLV的时间限制做处理
					/*if(record.getParam().get("xuqly").toString().equals("JLV")){
						flag = this.dealCLVData(record.getLineNum(),record.getParam().get("xuqcfsj").toString());
						if(flag==false){
							return flag;
						}
					}*/
					//获取车间号,线号前三位
					String chejh = record.getString("usercenter").concat(record.getParam().get("chejh").toString());
					record.put("shiycj", chejh);
					//过滤作用域
					Map<String,String> params = new HashMap<String,String>();
					params.put("xuqly", record.getParam().get("xuqly").toString());
					params.put("chejh", chejh);
					params.put("lingjbh", record.get("lingjbh").toString());
					params.put("usercenter", record.get("usercenter").toString());
					flag = this.filtrateZuoyy(params);
					if(flag==false){
						return flag;
					}
					//版次格式化
					String xuqly =  record.getParam().get("xuqly").toString();
					String xuqbc = this.formateXuqbc(xuqly);
					String usercenter = record.get("usercenter").toString();
					//存放需求来源key/value，子线程根据需求来源取需求版次 hzg 2013-4-15
					record.putParamObject(xuqly,xuqbc);
					if(!set.contains(xuqbc)){//不包含新的需求版次就增加
						//插入版次信息到毛需求主表
						this.insertMaoxq(xuqbc, record.getParam().get("xuqcfsj").toString(), xuqly,usercenter);
						set.add(xuqbc);
					}
				}else{
					logger.error("接口" + interfaceId + "第" + record.getLineNum() + "行数据错误 ,需求日期为空");
					return false;
				}
		} catch (RuntimeException e) {
			logger.error("线程--接口" + interfaceId + "第" + record.getLineNum() + "行数据错误  " + e.getMessage());
			throw new WarmBusinessException("插入毛需求错误！"+e.getMessage());
		} 
		return true;
	}
	
	@Override
	public void after() {
		//1.根据需求版次更新单位和换算比例后的零件
		conversionDanw();
		//2.制造路线转换，更新
		queryZhizxl();
		zhizlxIsNullYicbj();
		//3.产线转换,更新
		queryMubChanx();
		// 4.周,周期转换
		this.transformZhouq();
	}
	
	
	/**
	 * 单位换算
	 * @author Hezg
	 * @date 2013-4-17
	 */
	@SuppressWarnings("unchecked")
	public void conversionDanw(){
		logger.debug("查询单位换算");
		try{
				//(1).查询得到单位换算后的零件单位和需求数量 hzg 2013-4-17
				List<Map<String,Object>> lingjdwList =  dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.select("inPutzbc.queryDanwXuqslOfMaoxqmx",biaoz);
				//(2).更新毛需求零件和需求数量
				for(Map<String,Object> map : lingjdwList){
					/*if(map.get("FLAG").toString().equals("0")){ 
						saveYicbj("换算比例查询为空,单位"+map.get("DANW").toString()+",被换单位"
								+map.get("BEIHSDW").toString(),map.get("LINGJBH").toString(), map.get("USERCENTER").toString());
					}*/
					dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
					.execute("inPutzbc.updateDanwXuqsl", map);
				}
		}catch(RuntimeException e)
		{
			logger.error("线程--接口" + interfaceId +"方法conversionDanw更新单位换算报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"方法conversionDanw更新单位换算报错"+e.getMessage());
		}
	}
	
	/**
	 * 制造路线定货路线转换
	 * @author Hezg
	 * @date 2013-4-17
	 */
	@SuppressWarnings("unchecked")
	public void queryZhizxl(){
		logger.debug("查询制造路线");
		try{
				String count = (String)dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.selectObject("inPutzbc.queryZhizlxDinghlxOfMaoxqmxCount",biaoz);
				if(Integer.parseInt(count)>0){
					List<Map<String,String>> zhizlxList = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
					.select("inPutzbc.queryZhizlxDinghlxOfMaoxqmxCuowu",biaoz);
					for(Map<String,String> map : zhizlxList){ //只记录前4条到异常报警表
						//saveYicbj("订货路线查询无数据，共"+count+"条无订货路线,现制造路线"+map.get("ZHIZLXX"), map.get("LINGJBH"), map.get("USERCENTER"));
						logger.info("订货路线查询无数据，共"+count+"条无订货路线,现制造路线"+map.get("ZHIZLXX"));
					}
				}
				List<Map<String,String>> zhizlxList = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.select("inPutzbc.queryZhizlxDinghlxOfMaoxqmx",biaoz);
				for(Map<String,String> map :zhizlxList){
					dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
					.execute("inPutzbc.updateZhizlxDinghlx", map);
				}
		}catch(RuntimeException e)
		{
			logger.error("线程--接口" + interfaceId +"方法queryZhizxl更新制造路线转换报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"方法queryZhizxl更新制造路线转换报错"+e.getMessage());
		}
	}
	
	/**
	 * 原制造路线转现制造路线为空报警信息
	 * @author Hezg
	 * @date 2013-4-17
	 */
	@SuppressWarnings("unchecked")
	public void zhizlxIsNullYicbj(){
		try{
				//查询现制造路线为空的记录数，只记录前4条到异常报警表
				String zhizlxCount =  (String)dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.selectObject("inPutzbc.queryLingjbhZhizlxxIsNullOfMaoxqmxCount",biaoz);
				List<Map<String,String>> zhizlxList =  dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.select("inPutzbc.queryLingjbhZhizlxxIsNullOfMaoxqmx",biaoz);
				for(Map<String,String> map: zhizlxList){
					logger.info("现制造路线查询无数据，共"+zhizlxCount+"条无现制造路线,原制造路线"
							+map.get("ZHIZLX")+"零件编号"+map.get("LINGJBH")+"用户中心"+map.get("USERCENTER"));
				}
		}catch(RuntimeException e)
		{
			logger.error("线程--接口" + interfaceId +"方法zhizlxIsNullYicbj原制造路线转现制造路线空报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"方法zhizlxIsNullYicbj原制造路线转现制造路线空报错"+e.getMessage());
		}
	}
	
	/**
	 * 查询目标产线，并做转换
	 * @author Hezg
	 * @date 2013-4-17
	 */
	@SuppressWarnings("unchecked")
	public void queryMubChanx(){
		try{
				String diffChanxNum = (String)dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.selectObject("inPutzbc.queryDiffChanxCount",biaoz) ;
				if(Integer.parseInt(diffChanxNum)>0){//如果有不同的产线则进行更新
					List<Map<String,String>> mubcxList =  dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
					.select("inPutzbc.queryMubcxDiffChanx",biaoz);
					for(Map<String,String> map:mubcxList){
						dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
						.execute("inPutzbc.updateMubcxOfChanxhb", map);
					}
				}
		}catch(RuntimeException e)
		{
			logger.error("线程--接口" + interfaceId +"方法queryMubChanx目标产线查询更新报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"方法queryMubChanx目标产线查询更新报错"+e.getMessage());
		}
	}
	

	/**
	 * 格式化需求版次
	 * @author 贺志国
	 * @date 2012-10-16
	 * @param xuqly 需求来源
	 * @param calendar 当前日历对象
	 * @return String 需求版次号
	 */
	public String formateXuqbc(String xuqly){
		logger.debug("格式化需求版次");
		NumberFormat nf = NumberFormat.getInstance();
		nf.setMinimumIntegerDigits(2); 
		SimpleDateFormat yyyyMMdd = new SimpleDateFormat("yyyyMMdd"); 
		String xuqbc = xuqly+yyyyMMdd.format(calendar.getTime())+nf.format(liush);
		return xuqbc;
	}
	
	/**
	 * CLV数据处理
	 * @author 贺志国
	 * @date 2012-10-15
	 * @param rowIndex 行序号
	 * @param xuqly 需求来源
	 * @param xuqcfsj 需求拆分时间
	 * @param calendar 日历对象
	 */
	public boolean dealCLVData(int rowIndex,String xuqcfsj){
			logger.debug("JLV数据处理");
			Calendar calendar = Calendar.getInstance();
			//如果有CLV数据,进行清除
			if(rowIndex == 1){
//				//清除毛需求明细表数据
//				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzbc.maoxqmxDelOfCLV");
//				//清除毛需求表数据
//				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzbc.maoxqDelOfCLV");
			}
			//如果当前时间在中午12点之前（取昨天的数据，即跳过不为昨天的数据）
			SimpleDateFormat d_format = new SimpleDateFormat("yyyy-MM-dd"); 
			if(calendar.get(calendar.AM_PM) == calendar.AM){
				calendar.add(Calendar.DAY_OF_MONTH, -1);
				//如果拆分日期不为昨天,跳过该条 
				if(!DateTimeUtil.DateFormat(xuqcfsj).equals(d_format.format(calendar.getTime()))){
					return false;
				}
				//如果当前时间在中午12点之后（取今天的数据，即跳过不为今天的数据）
			}else{
				//如果拆分日期不为今天,跳过该条
				if(!DateTimeUtil.DateFormat(xuqcfsj).equals(d_format.format(calendar.getTime()))){
					return false;
				}
			}
			return true;
	}
	
	/**
	 * 过滤作用域
	 * @author 贺志国
	 * @date 2012-10-15
	 * @param xuqly 需求来源
	 * @param chejh 车间号
	 * @param lingjbh 零件编号
	 * @param usercenter 用户中心
	 */
	public boolean filtrateZuoyy(Map<String,String> params){
		String strZyy = (String)dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
		.selectObject("inPutzbc.queryZuoyyOfXuqly", params);
		//查询到作用域为空,过滤该数据
		if(ConvertUtils.strNull(strZyy).equals("")){
			//saveYicbj("作用域查询为空,需求来源"+params.get("xuqly")+"车间号"+params.get("chejh"), params.get("lingjbh"), params.get("usercenter"));
			logger.info("作用域查询为空,需求来源"+params.get("xuqly")+"车间号"+params.get("chejh"));
			return false;
		}
		return true;
	}
	
	
	/**
	 * 周,周期转换
	 * @author 贺志国
	 * @date 2012-10-15
	 * @param record 文本读取集合
	 * @param xuqrq 需求日期
	 * @return void
	 */
	@SuppressWarnings("unchecked")
	public void transformZhouq(){
		logger.debug("周,周期转换");
	try{
		//周,周期转换
		List<Map<String,String>> zxzqList = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
		.select("inPutzbc.queryZxZqOfCalendarCenter",biaoz);
    	//如果查询不到相关周期,周序信息,异常报警
    	if(zxzqList.isEmpty()){
    		//saveYicbj("中心日历表周,周期信息查询无数据","","");
    		logger.info(interfaceId +" 周,周期转换 ,中心日历表周,周期信息查询无数据");
    	}else{
    		for(Map<String,String> map:zxzqList){
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutzbc.updateZxZqOfmaoxqmx", map);
			}
    	} 	
	}catch(RuntimeException e)
	{
		logger.error("线程--接口" + interfaceId +"方法transformZhouq目标产线查询更新报错"+e.getMessage());
		throw new ServiceException("线程--接口" + interfaceId +"方法transformZhouq目标产线查询更新报错"+e.getMessage());
	}
	}
	

	/**
	 * 毛需求表插入版次信息
	 * @author 贺志国
	 * @date 2012-10-15
	 * @param xuqbc 需求版次
	 * @param xuqcfsj 需求拆分时间
	 * @param xuqly 需求来源
	 */
	public void insertMaoxq(String xuqbc,String xuqcfsj,String xuqly,String usercenter){
		logger.debug("毛需求表插入版次信息");
		Map<String,String> params = new HashMap<String,String>();
		params.put("xuqbc",xuqbc);
		params.put("xuqcfsj", xuqcfsj);
		params.put("xuqly", xuqly);
		params.put("usercenter", usercenter);
		params.put("create_time", DateTimeUtil.getAllCurrTime());
		params.put("edit_time", DateTimeUtil.getAllCurrTime());
		try{
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.execute("inPutzbc.insertMaoxqPJ", params);
		}catch(RuntimeException e)
		{
			logger.error("线程--接口" + interfaceId +"插入毛需求表插入版次信息xqjs_maoxq表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"插入毛需求表插入版次信息xqjs_maoxq表时报错"+e.getMessage());
		}
	}


	/**
	 * 保存异常报警
	 * @param cuowxxxx 错误详细信息
	 * @param lingjbh 零件编号
	 * @param usercenter 用户中心
	 */
	/*public void saveYicbj(String cuowxxxx,String lingjbh,String usercenter){
		logger.debug(" 保存异常报警");
		Map<String,String> params = new HashMap<String,String>();
		params.put("cuowxxxx", cuowxxxx);
		params.put("lingjbh", lingjbh);
		params.put("usercenter", usercenter);
		params.put("create_time", DateTimeUtil.getAllCurrTime());
		params.put("edit_time", DateTimeUtil.getAllCurrTime());
		dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzbc.insertYicbj", params);
	}*/
	
	
}
