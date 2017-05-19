package com.athena.component.input;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.athena.component.exchange.FileLog;
import com.athena.component.exchange.ParserException;
import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.field.DataField;
import com.athena.component.exchange.field.DateFieldFormat;
import com.athena.component.exchange.field.NumberFieldFormat;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.util.exception.ServiceException;
import com.toft.core3.transaction.annotation.Transactional;
import com.toft.utils.StringUtils;
import com.toft.utils.UUIDHexGenerator;

/**
 * 2521 EFI同步要货令
 * @date 2013-7-30
 * @author hzg
 *
 */
public class TongbyhlDBDateWriter extends TxtInputDBSerivce{
	 private String datasourceReaderId = "";  
	 private String datasourceWriterId = "";
	 private String visule = "J";

		public TongbyhlDBDateWriter(DataParserConfig dataParserConfig) {
			super(dataParserConfig);
			datasourceReaderId = dataParserConfig.getReaderConfig().getDatasourceId();
			datasourceWriterId = dataParserConfig.getWriterConfig().getDatasourceId();
		}
		
		/**
		 * 接口处理之前更新ck_tongbjpd表的editor状态F改为'J'(待处理状态)
		 */
		@Override
		public void before(){
			try{
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceReaderId)
				.execute("inPutzxc.updateTongbjpdZTBefore");
			}catch(RuntimeException e){
				logger.error("接口" + interfaceId +"更新ck_tongbjpd表editor状态F为J时报错"+e.getMessage());
				throw new ServiceException("接口" + interfaceId +"更新ck_tongbjpd表editor状态F为J时报错"+e.getMessage());
			}
		}
		
		/**
		 * 读取DB数据,解析数据后放入Record中
		 */
		@Override
		@SuppressWarnings("unchecked")
		protected boolean readListDB () {
			logger.info("接口" + interfaceId + "开始读取DB数据");
			long startTime = System.currentTimeMillis();	
			int lineCount = 0;
			try {
				//1、查询要发送的要货令（包装单元卡）
				List<Map<String,String>> dataMainList = dataParserConfig.getBaseDao().getSdcDataSource(datasourceReaderId).
				select("inPutzxc.queryEFIyhlMain");
				//2、根据要发送的要货令（包装单元卡）获取同步要货令（流水号）
				for(Map<String,String> map : dataMainList){
					List<Map<String,Object>>  dataMxList = dataParserConfig.getBaseDao().getSdcDataSource(datasourceReaderId).
					select("inPutzxc.queryEFITongbyhl",map.get("YAOHLH").toString());
					lineCount+=dataMxList.size();
					logger.info("包装单元卡"+map.get("YAOHLH").toString()+"对应的要货令明细有"+dataMxList.size()+"条");
					//3、解析数据后放入Record中
					List<Record> recordList = putDataToRecord(dataMxList);
					//4、将数据写到EFI200表中
					insertEFI200(recordList);
					//5、将同步集配单表中editor状态改为visule(T)
					updateStateOfTongbyhl(map.get("YAOHLH").toString());
				}
			} catch (RuntimeException e) {
				logger.error("接口" + interfaceId + "执行sql异常" + e.getMessage(),e);
				throw new ServiceException("线程--接口" + dataParserConfig.getId() +"执行sql异常"+e.getMessage(),e);
			} 
			logger.info("数据写入EFI200表完成，共解析记录"+lineCount+"条,耗时"+(System.currentTimeMillis()-startTime)+"毫秒.");
			return true; 
		}

		
		/**
		 * 将查询到的结果集解析后放到record中
		 * @author 贺志国
		 * @date 2013-9-4
		 * @param dataMxList 查询到的DB结果集
		 * @return List<Record> 解析后的结果集
		 */
		public List<Record> putDataToRecord(List<Map<String,Object>>  dataMxList){
			List<Record> lineList = new ArrayList<Record>();
			int lineNum =0;
			for(Map<String,Object> map: dataMxList){
				lineNum++;
				Record record = null;
				record = parserDbDate(map);
				record.setLineNum(lineNum);
				lineList.add(record);
			}
			return lineList;
		}
		
		/**
		 * 向EFI200中写数据
		 * @author 贺志国
		 * @date 2013-9-4
		 * @param record
		 */
		@Transactional
		public void insertEFI200(List<Record> recordList){
			String yaohlh = "";
			for(Record record : recordList) {
				logger.info("接口" + interfaceId + " 解析包装单元卡号为"+record.getString("ORDERNO")+"，整车流水号为"+record.getString("CARSEQ")+"第" + record.getLineNum() + "行数据"+record.getValue());
				Map<String,String> params = putParams(record);
				try{
					dataParserConfig.getBaseDao().getSdcDataSource(datasourceWriterId)
					.execute("inPutzxc.insertEFI200",params);//插入数据到EFI200
					logger.info("接口" + interfaceId + "包装单元卡号为"+record.getString("ORDERNO")+"整车流水号为"+record.getString("CARSEQ")+"的要货令插入EFI200完成");
					yaohlh = record.getString("ORDERNO").toString();
				}catch(RuntimeException e){
					logger.error("接口" + interfaceId +"更新EFI200表时报错，包装单元卡号为"+record.getString("ORDERNO")+"整车流水号为"+record.getString("CARSEQ")+e.getMessage());
					//判断是否需要抛出异常通知autosys
					runSqlException(e,record);
					//throw new ServiceException("线程--接口" + interfaceId +"更新EFI200表时报错"+e.getMessage());
				}
			}
			//更新EFI表中的flag状态0改为1
			updateFlagStateOfEFI(yaohlh);
			visule ="T";
			logger.info("接口" + interfaceId + "本次更新的要货令号为"+yaohlh);
		}
		
		/**
	     * 判断是否需要抛出异常通知autosys
	     * @author Hezg
	     * @date 2013-2-19
	     */
	    public void runSqlException(RuntimeException sqlEx,Record record){
	    	String message = sqlEx.getMessage();
	        String [] oraStr = message.split(":");
	        if("ORA-00957".equals(oraStr[2].trim())
	        		||"ORA-00904".equals(oraStr[2].trim())
	        		||"ORA-00942".equals(oraStr[2].trim())
	        		||"ORA-12899".equals(oraStr[2].trim())
	        		||"ORA-01400".equals(oraStr[2].trim())
	        		||"ORA-01861".equals(oraStr[2].trim())
	        		||"ORA-01847".equals(oraStr[2].trim())){//可判断的SQL异常，报1，写日志数据库
	        	 //错误日志记录
	        	logger.error("接口" + interfaceId + "插入包装单元卡号为"+record.getString("ORDERNO")+"的第" + record.getLineNum() + "行数据"+record.getString("CARSEQ")
	        			+ "  执行sql异常" + sqlEx.getMessage(), sqlEx);
	            in_errorfile_info(record,sqlEx.getMessage());
	        	throw new ServiceException("接口" + interfaceId + "插入包装单元卡号为"+record.getString("ORDERNO")+"的第" + record.getLineNum() + "行数据"+record.getString("CARSEQ")
	                    + "  执行sql异常" + sqlEx.getMessage(), sqlEx);
	        }else if("ORA-00001".equals(oraStr[2].trim())){//出现ORA-00001: 违反唯一约束条件可忽略SQL异常，写错误日志文件，写表
	        	logger.error("业务忽略错误:接口" + interfaceId + " 执行sql异常，违反唯一约束条" + sqlEx.getMessage(), sqlEx);
	        	in_errorfile_info(record,sqlEx.getMessage());
	        }else{ //其他不可判断的SQL异常也报1，写日志数据库
	        	 //错误日志记录
	            in_errorfile_info(record,sqlEx.getMessage());
	            throw new ServiceException("接口" + interfaceId + "插入包装单元卡号为"+record.getString("ORDERNO")+"的第" + record.getLineNum() + "行数据"+record.getString("CARSEQ")
	                    + "  执行sql异常" + sqlEx.getMessage(), sqlEx);
	        }
	    }
		
	    /**
		 * 向 接口文件错误记录信息 表中 记录日志
		 * @date 2013-2-18
		 * @param errorNameMap  错误文件名
		 * @param errorMeList 错误信息集合
		 */
	    private void in_errorfile_info(Record record,String error_date) {
	    	Map<String,String> params = new HashMap<String,String>();
	    	params.put("EID", UUIDHexGenerator.getInstance().generate());
	    	params.put("SID", "");
	    	params.put("INBH", interfaceId);
	    	params.put("file_errorinfo",StringUtils.ifNull(record.getValue()));
	    	params.put("error_date", StringUtils.ifNull(error_date).substring(0, 290));
	    	FileLog.getInstance(datasourceReaderId).insert_file_ErrorInfo(params,dataParserConfig.getBaseDao());
		}

		/**
		 * 更新EFI200中的flag为0的状态为1
		 * @author 贺志国
		 * @date 2013-9-4
		 * @param yaohls 要货令号
		 */
		public void updateFlagStateOfEFI(String yaohl){
			try{
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceWriterId)
				.execute("inPutzxc.updateFlagOfEfi200",yaohl);
			}catch(RuntimeException e){
				logger.error("接口" + interfaceId +"更新EFI200表flag状态0为1时报错"+e.getMessage());
				throw new ServiceException("接口" + interfaceId +"更新EFI200表flag状态0为1时报错"+e.getMessage());
			}
		}

		/**
		 * 更新ck_tongbjpd表中的editor状态为变量visual
		 * @author 贺志国
		 * @date 2013-9-4
		 * @param recordList
		 */
		public void updateStateOfTongbyhl(String yaohlh){
			Map<String,String> params = new HashMap<String,String>();
			params.put("visule", visule);
			params.put("yaohlh", yaohlh);
			try{
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceReaderId)
				.execute("inPutzxc.updateStateOfTongbjpd",params);
			}catch(RuntimeException e){
				logger.error("接口" + interfaceId +"更新ck_tongbjpd表editor状态J为F时报错"+e.getMessage());
				throw new ServiceException("接口" + interfaceId +"更新ck_tongbjpd表editor状态J为F时报错"+e.getMessage());
			}
		}
		
		/**
		 * 封装参数
		 * @author 贺志国
		 * @date 2013-9-4
		 * @param record
		 * @return
		 */
		public Map<String,String> putParams(Record record){
			Map<String,String> params = new HashMap<String,String>();
			params.put("ORDERNO",record.getString("ORDERNO"));
			params.put("UC",record.getString("UC"));
			params.put("DELIVERTIME",record.getString("DELIVERTIME"));
			params.put("FIRSTTIME",record.getString("FIRSTTIME"));
			params.put("LASTTIME",record.getString("LASTTIME")); 
			params.put("VENDER",record.getString("VENDER")); 
			params.put("PARTNO",record.getString("PARTNO")); 
			params.put("METRICUNIT",record.getString("METRICUNIT")); 
			params.put("AMOUNT",record.getString("AMOUNT"));
			params.put("UATYPE",record.getString("UATYPE"));
			params.put("UAPACKAMOUNT",record.getString("UAPACKAMOUNT"));
			params.put("UCTYPE",record.getString("UCTYPE"));
			params.put("UCPACKAMOUNT",record.getString("UCPACKAMOUNT"));
			params.put("TRANTIME",record.getString("TRANTIME"));
			params.put("UNLOADLOCAL",record.getString("UNLOADLOCAL"));
			params.put("CONSUMELOCAL",record.getString("CONSUMELOCAL"));
			params.put("PLANNO",record.getString("PLANNO"));
			params.put("ORDERTYPE",record.getString("ORDERTYPE"));
			params.put("SYNNUMBER",record.getString("SYNNUMBER"));
			params.put("WORKNO",record.getString("WORKNO"));
			params.put("LINETYPE",record.getString("LINETYPE"));
			params.put("LINENO",record.getString("LINENO"));
			params.put("CLIENT",record.getString("CLIENT"));
			params.put("DELIVERTYPE",record.getString("DELIVERTYPE"));
			params.put("CREATTIME",record.getString("CREATTIME"));
			params.put("PARTSEQ",record.getString("PARTSEQ"));
			params.put("CARSEQ",record.getString("CARSEQ"));
			params.put("CARTIME",record.getString("CARTIME"));
			params.put("SYNPLACE",record.getString("SYNPLACE"));
			params.put("SYNPARTNO",record.getString("SYNPARTNO"));
			params.put("SUPPLIER",record.getString("SUPPLIER"));
			params.put("SUPPLIERNAME",record.getString("SUPPLIERNAME"));
			params.put("SYNPARTNUM",record.getString("SYNPARTNUM"));
			params.put("SYNUNIT",record.getString("SYNUNIT"));
			params.put("XH",record.getString("XH"));
			params.put("BLFLAG",record.getString("BLFLAG"));
			params.put("ROLLNO",record.getString("ROLLNO"));
			params.put("CREATDATE",record.getString("CREATDATE"));
			params.put("FLAG","0");
			return params;
		}

		
		/**
		 * 解析数据库读取的数据行
		 * @author Hezg
		 * @date 2013-3-14
		 * @param map 数据行记录
		 */
		private Record parserDbDate(Map<String,Object> map){
			Record record = new Record();
			//得到所需入库的字段
	    	DataField[] dataFileds = dataParserConfig.getDataFields();
	    	for(DataField dataField:dataFileds){
	    		String strValue = "";
				try {
					if(dataField.getIsParam()){ //放存要入库的字段和值  ，2013-4-1 hzg db->db的数据不进行转换，直接放record中入库
						//record.put(dataField.getWriterColumn(),strNull(map.get(dataField.getReaderColumn())));
						//修改，如果是number或date类型，则进行转换 hzg 2013-7-18 update ,number转换有问题
						record.put(dataField.getWriterColumn(),convertValue(dataField,strNull(map.get(dataField.getReaderColumn()))));
					}
				} catch (ParserException e) {
					logger.error("接口" + interfaceId + "解析 第" + map
	                        + "行数据" + dataField.getWriterColumn() + ":" + strValue.trim() + "转换异常  " + e.getMessage());
					//程序转换出错抛异常 1 hzg 2013-7-22
					throw new ServiceException("接口" + interfaceId + "解析 第" + map
	                        + "行数据" + dataField.getWriterColumn() + ":" + strValue.trim() + "转换异常  " + e.getMessage(),e);
				}
			}
			return record;
		}
		
		/**
		 * 数据格式转换，如果数据值为空则直接返回
		 * @param dataField 输出字段域
		 * @param strValue 输出值
		 * @return Object
		 * @throws ParserException 数据转换异常
		 */
		private Object convertValue(DataField dataField, Object strValue) throws ParserException {
			String type = dataField.getType();
			if(!"".equals(strValue)&&null!=type){
				if("number".equalsIgnoreCase(type)){
					strValue = new NumberFieldFormat(dataField.getFormat()).parse(strValue.toString());
				}else if("date".equalsIgnoreCase(type)){//日期格式
					strValue = new DateFieldFormat(dataField.getFormat()).parse(strValue.toString());
				} 
			}
			return strValue;
		}
		
		/**
		 * 空串处理
		 * 
		 * @param obj
		 *            对象
		 * @return 处理后字符串
		 * @author GJ
		 * @date 2011-10-26
		 */
		private Object strNull(Object obj) {// 对象为空返回空串,不为空toString
			return obj == null ? "" : obj;
		}
}
