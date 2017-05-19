package com.athena.component.input;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.athena.component.DateTimeUtil;
import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.component.exchange.txt.WarmBusinessException;
import com.athena.component.exchange.utils.ConvertUtils;
import com.athena.util.exception.ServiceException;


/**
 * 解析数据之前的处理方法
 * @date 2013-2-19
 * @author hzg
 */
public class DdxxDbDataReader extends TxtInputDBSerivce{
	protected static Logger logger = Logger.getLogger(DdxxDbDataReader.class);	//定义日志方法
	public DdxxDbDataReader(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
	}

	/**
	 * 数据解析之前清空零件in_ddxx表数据
	 */
	@Override
	public void before() {
		try{
			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.execute("inPutzbc.ddxxDelete");
		}catch(RuntimeException e)
		{
			logger.error("线程--接口" + interfaceId +"清除in_ddxx表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"清除in_ddxx表时报错"+e.getMessage());
		}
	}
	
	/**
	 * 行记录解析前回调
	 * @param line 行数据
	 * @param fileName 文件名
	 * @param lineNum 行数
	 */
	@Override
	public boolean beforeRecord(String line, String fileName, int lineNum){
		boolean result = true;
		String rowStr=line.toString().substring(0,3);
		if(lineNum==1){ //过滤掉第一行
			result = false;
		}
		if(rowStr.indexOf("END")!=-1){//行数据中含END的过滤掉
			result = false;
		}
		return result;
	}
	
	
	/**
	 * 行解析之后处理方法
	 * @param rowIndex 行标
	 * @param record 行数据集合
	 * @author HZG
	 * @modify 如果订单号超过8位则不接收 hzg 2014.6.6  
	 */
	@Override
	public boolean afterRecord(Record record) { 
		boolean isBool = true;
		String dingdh = record.getString("dingdh");
		if(dingdh.length()>8){
			isBool = false;
		}else{
			record.put("dingdh", dingdh.substring(0, 8));
			record.put("shul", Integer.parseInt(record.getString("shul").substring(0, 10).trim()));
			try{ 
				//从供应商参考系表中获取供应商类型存入集合 
				//record.put("leix", this.queryLeixOfGongys(record.getString("usercenter"), record.getString("gysdm")));
				//鲁鸣 确定供应商类型为外部2 ，因此这里不需要去查询供应商类型 hzg 2014.6.10
				record.put("leix", "2");
				

				//从零件仓库设置参考系表中获取卸货站台编号存入集合 
				record.put("xiehztbh", this.queryXiehztbhOfLingjck(
						record.getString("usercenter"), 
						record.getString("lingjbh"), 
						record.getString("ckdm")
				));

				//从零件表中获得‘订单车间’存到接口表中 
				record.put("dhcj", this.queryDinghcjOfLingj(
						record.getString("usercenter"), 
						record.getString("lingjbh")
				)
				);			

				//从零件-供应商参考系表中获取UA型号，UC型号，UA中UC容量，UC中零件容量
				List<Map<String,String>> map = this.queryUAOfLingjgys(
						record.getString("usercenter"), 
						record.getString("lingjbh"), 
						record.getString("gysdm")
				);
				if(!map.isEmpty()){
					//获取UA型号存入集合
					record.put("uabzlx", map.get(0).get("UABZLX"));
					//获取UC型号存入集合
					record.put("ucbzlx", map.get(0).get("UCBZLX"));
					//获取UA中UC容量存入集合
					record.put("ucrl", map.get(0).get("UCRL")); 
					//获取UC中零件容量存入集合
					record.put("uaucgs", map.get(0).get("UAUCGS"));
				}

				//获得要货起始日期
				String strYhqsDate=record.getString("yhqs_date").trim();
				if(StringUtils.isNotEmpty(strYhqsDate)){
					Date yhqs_date = this.dealYhqsrqStringToDate(strYhqsDate);
					record.put("yhqs_date", yhqs_date);//格式化的结果集存入表字段中
					record.put("jf_date", yhqs_date);//交付日期与要货起始日期一致
				}

				//获得要货结束日期
				String strYhjsDate=record.getString("yhjs_date").trim();
				if(StringUtils.isNotEmpty(strYhjsDate)){
					Date yhjs_date = this.dealYhjsrqStringToDate(strYhjsDate);
					record.put("yhjs_date", yhjs_date);//格式化的结果集存入表字段中
				}	

				//从外部物流参考系表中获取路径编号,如果存在则存入集合 
				record.put("lujbh", this.queryLujbhOfWaibwl(
						record.getString("usercenter"),  
						record.getString("ckdm"), 
						record.getString("gysdm")
				));
			}catch(Exception e){
				logger.error("线程--接口" + interfaceId + "第" + record.getLineNum() + "行数据解析错误  " + e.getMessage());
				throw new WarmBusinessException("日期转换错误！"+e.getMessage());
			}

			//获得当前系统时间
			record.put("zhwhsj", new Date());
			//存入创建时间和处理状态初始值
			record.put("cj_date", new Date());
			record.put("clzt", 0);	
		}
		return isBool;
	}
	
	/**
	 * 根据’用户中心’，’供应商代码’从供应商参考系表中获取供应商类型
	 * @author 贺志国
	 * @date 2012-10-23
	 * @param usercenter 用户中心
	 * @param gysdm 供应商代码
	 * @return String 供应商类型
	 */
	public String queryLeixOfGongys(String usercenter,String gysdm){
		Map<String,String> params = new HashMap<String,String>();
		params.put("usercenter", usercenter);
		params.put("gysdm", gysdm); 
		return ConvertUtils.strNull((String)dataParserConfig.getBaseDao().selectObject("inPutzbc.queryLeixOfGongys",params));
	}
	
	/**
	 * 根据’用户中心’，’零件号’，’仓库代码’从零件仓库参考系表中获取卸货站台编号
	 * @author 贺志国
	 * @date 2012-10-23
	 * @param usercenter 用户中心
	 * @param lingjbh 零件编号
	 * @param ckdm 仓库代码
	 * @return String 卸货站台编号
	 */
	public String queryXiehztbhOfLingjck(String usercenter,String lingjbh,String ckdm){
		Map<String,String> params = new HashMap<String,String>();
		params.put("usercenter", usercenter);
		params.put("lingjbh", lingjbh);
		params.put("cangkbh", ckdm); 
		return ConvertUtils.strNull((String)dataParserConfig.getBaseDao().selectObject("inPutzbc.queryXiehztbhOfLingjck",params));

	}
	
	/**
	 * 根据’用户中心’,’零件编号’从零件表参考系中获取订货车间
	 * @author 贺志国
	 * @date 2012-10-23
	 * @param usercenter 用户中心
	 * @param lingjbh 零件编号
	 * @return String 订货车间
	 */
	public String queryDinghcjOfLingj(String usercenter,String lingjbh){
		Map<String,String> params = new HashMap<String,String>();
		params.put("usercenter", usercenter);
		params.put("lingjbh", lingjbh); 
		return ConvertUtils.strNull((String)dataParserConfig.getBaseDao().selectObject("inPutzbc.queryDinghcjOfLingj",params));
	}
	
	/**
	 * 根据’用户中心’，’零件号’，’供应商代码’从零件-供应商表中获取供应商UA包装类型、UC包装类型、UC容量、UA里UC的个数
	 * @author 贺志国
	 * @date 2012-10-23
	 * @param usercenter
	 * @param lingjbh
	 * @param gysdm
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public List<Map<String,String>> queryUAOfLingjgys(String usercenter,String lingjbh,String gysdm){
		Map<String,String> params = new HashMap<String,String>();
		params.put("usercenter", usercenter);
		params.put("lingjbh", lingjbh);
		params.put("gongysbh", gysdm); 
		return dataParserConfig.getBaseDao().select("inPutzbc.queryUAOfLingjgys",params);
	}
	
	/**
	 * 根据’用户中心’,’目的地’,’供应商代码’从外部物流表中获取物流路径编号
	 * @author 贺志国
	 * @date 2012-10-23
	 * @param usercenter 用户中心
	 * @param ckdm 卸货站台编号
	 * @param gysdm 供应商代码
	 * @return String 物流路径编号
	 */
	public String queryLujbhOfWaibwl(String usercenter,String ckdm,String gysdm){
		Map<String,String> params = new HashMap<String,String>();
		params.put("usercenter", usercenter);
		params.put("ckdm", ckdm);
		params.put("gongysbh", gysdm); 
		return ConvertUtils.strNull((String)dataParserConfig.getBaseDao().selectObject("inPutzbc.queryLujbhOfWaibwl",params));
	}
	
	
	/**
	 * 要货开始日期格式化
	 * @author 贺志国
	 * @date 2012-10-23
	 * @param yhqsrq 要货开始日期字符串
	 * @return Date 格式过的日期 
	 */
	public Date dealYhqsrqStringToDate(String yhqsrq) throws ParseException{
		SimpleDateFormat formatYMDhms = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		Date dateFormat = null;
		if(StringUtils.isNotEmpty(yhqsrq)){
			String subStringDate = DateTimeUtil.SubString(yhqsrq);
			try {
				//要货开始日期格式化
				dateFormat = formatYMDhms.parse(subStringDate);
			} catch (ParseException e) {
				logger.error(e.getMessage());
			}
		}

		return dateFormat;
	}
	
	/**
	 * 要货结束日期格式化
	 * @author 贺志国
	 * @date 2012-10-23
	 * @param yhjsrq 要货结束日期字符串
	 * @return Date 格式过的日期 
	 */
	public Date dealYhjsrqStringToDate(String yhjsrq) throws ParseException{
		SimpleDateFormat formatYMD = new SimpleDateFormat("yyyy-MM-dd");
		Date dateFormat = null;
		if(StringUtils.isNotEmpty(yhjsrq)){
			String strgDate = DateTimeUtil.DateStr(yhjsrq);
			try {
				//要货结束日期格式化
				dateFormat = formatYMD.parse(strgDate);
			} catch (ParseException e) {
				logger.error(e.getMessage());
			}
		}

		return dateFormat;
	}
	

}
