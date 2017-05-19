package com.athena.component.input;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.athena.component.DateTimeUtil;
import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtWriterDBTask;
import com.athena.component.exchange.txt.WarmBusinessException;

/**
 * 2030 GEVP外部要货令
 * @author hzg
 *
 */
public class GEVPYhlDbDataWriter extends TxtWriterDBTask{
	protected static Logger logger = Logger.getLogger(GEVPYhlDbDataWriter.class);	//定义日志方法
	
	public GEVPYhlDbDataWriter(DataParserConfig dataParserConfig, List<String> fieldList, List<String> updateFieldList, String fileName, List<Record> lineList) {
		super(dataParserConfig, fieldList, updateFieldList, fileName, lineList); 
	}

	/**
	 * 行解析之后处理方法
	 * @param record 行数据集合
	 */
	@Override
	public boolean beforeRecord(Record record){		
			SimpleDateFormat dateformat=new SimpleDateFormat("yyyy-MM-dd HH:mm");
			if(null!=record){	
				//供应商代码
				String gysdm=record.getString("GYSDM").trim();
				if(StringUtils.isNotEmpty(gysdm)){
					record.put("GYSDM", gysdm);
				}

				//最晚交付时间
				String zuiwsj=record.getString("ZUIWSJ").trim();
				if(StringUtils.isNotEmpty(zuiwsj)){
					String d_zuiwsj=DateTimeUtil.DateFormat_Fhtz(zuiwsj);
					try {
						Date w_date = dateformat.parse(d_zuiwsj);
						record.put("ZUIWSJ",w_date);
					} catch(ParseException e){
						logger.error("线程--接口" + interfaceId + "第" + record.getLineNum() + "行数据时间转换异常  " + e.getMessage());
						throw new WarmBusinessException("日期转换错误！"+e.getMessage());
					} 
				}

				//零件编码
				String ljbm=record.getString("LINGJBH").trim();
				if(StringUtils.isNotEmpty(ljbm)){
					record.put("LINGJBH", ljbm);
				}

				//单位
				String danw=record.getString("DANW").trim();
				if(StringUtils.isNotEmpty(danw)){
					record.put("DANW", danw);
				}

				//UC型号
				String ucxh=record.getString("UCXH").trim();
				if(StringUtils.isNotEmpty(ucxh)){
					record.put("UCXH", ucxh);
				}

				//UC个数
				String ucgs=record.getString("UCGS").trim();
				if(StringUtils.isNotEmpty(ucgs)){
					int i_ucgs =Integer.parseInt(ucgs);
					record.put("UCGS", i_ucgs);
				}

				//卸货点
				String xiehd=record.getString("XIEHD").trim();
				if(StringUtils.isNotEmpty(xiehd)){
					record.put("XIEHD", xiehd);
				}
				//目的地
				String mudd=record.getString("MUDD").trim();
				if(StringUtils.isNotEmpty(mudd)){
					record.put("MUDD",mudd);
				}

				//订单号
				String dingdh=record.getString("DINGDH").trim();
				if(StringUtils.isNotEmpty(dingdh)){
					record.put("DINGDH", dingdh);
				}

				//客户
				String kehu=record.getString("KEHU").trim();
				if(StringUtils.isNotEmpty(kehu)){
					record.put("KEHU", kehu);
				}

				//插入创建日期和处理状态
				record.put("CJ_DATE",new Date());
				record.put("CLZT",0);

			}
			return true;
	}
}
