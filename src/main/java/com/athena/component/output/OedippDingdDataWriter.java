package com.athena.component.output;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.athena.component.exchange.ParserException;
import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.field.DataField;
import com.athena.component.exchange.field.DateFieldFormat;
import com.athena.component.exchange.field.NumberFieldFormat;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.util.exception.ServiceException;

/**
 * 自写义类，1022接口
 * @author 贺志国
 * @E-mail zghe@isoftstone.com
 * @version v1.0
 * @date 2013-11-28
 */
public class OedippDingdDataWriter extends TxtInputDBSerivce {
	private static Logger logger = Logger.getLogger(OedippDingdDataWriter.class);
	private final static int LINE_NUM = 10000;
	private static final int TRUE = 1;
    private static final int FALSE = 0;
	public OedippDingdDataWriter(DataParserConfig dataParserConfig){
		super(dataParserConfig);
	}
	
	
	/**
	 * 重写父类方法，不分页查询
	 * @date hzg 2013-11-28
	 */
	@SuppressWarnings("unchecked")
	@Override
	public boolean readListDB(){
		try{
			List<Map<String,Object>> dataList = dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getReaderConfig().getDatasourceId())
			.select(dataParserConfig.getReaderConfig().getSql());
			readFileDB(dataList);
		}catch (Exception e){
			logger.error(e.getMessage());
			throw new ServiceException(e);
		}
		return true;
	}
	
	
	/**
     * 读取解析DB数据
     * @param listdb 数据库集合
     * @return 数字 1 表示成功 ，数字 0 表示失败
     */
	protected boolean readFileDB(List<Map<String,Object>> listdb){
        boolean result = false;
    	List<Record> lineList = new ArrayList<Record>();
        int lineNum = 0;
        long startTime = System.currentTimeMillis();	
    	file_begintime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
		List<Future<Boolean>> list = new ArrayList<Future<Boolean>>();
		for(Map<String,Object> map: listdb){
			lineNum++;
			Record record = null;
			record = parserDbDate(map);
			record.setLineNum(lineNum);
			lineList.add(record);
			if(!afterRecord(record)){
				continue;
			}
		    if (lineList.size() == LINE_NUM) {
		        list.add(exeTask("DB", lineNum, lineList));
		        lineList = new ArrayList<Record>();
		    }
		}
		if (lineList.size() > 0) {
		    list.add(exeTask("DB", lineNum, lineList));
		}
		//设置文件解析结束时间
		file_endtime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
		result = collectTask(list); 
        logger.info("接口" + interfaceId + "读DB结束 , 总记录行数" + lineNum);
    	logger.info("文件解析完成，共解析记录"+lineNum+"条,耗时"+(System.currentTimeMillis()-startTime)+"毫秒.");
        return result;
    }  
	
	
	 /**
     * 合并子线程的结果
     * @param list   子线程列表
     * @return 子线程的结果
     */
    private boolean collectTask(List<Future<Boolean>> list) {
        int count = 0;
        for (Future<Boolean> future : list) {
            try {
                count += future.get() ? TRUE : FALSE;
            } catch (InterruptedException e) {
                logger.error("接口" + interfaceId + "读取文件出现线程中断" + e.getMessage());
            } catch (ExecutionException e) {
                throw launderThrowable(e);
            } finally {
                future.cancel(true);
            }
        }
        return count == list.size() ? true : false;
    }
    
    
    /**
     * 
     * @author Hezg
     * @date 2013-3-21
     * @param t  java.util.concurrent.ExecutionException
     * @return
     */
    private RuntimeException launderThrowable(Throwable t) {
    	System.out.println(t.getCause());
    	if (t.getCause() instanceof ServiceException)
    		return (ServiceException) t.getCause();
    	else if (t.getCause() instanceof RuntimeException)
            return (RuntimeException) t.getCause();
    	 else if (t.getCause() instanceof NullPointerException)
             throw (NullPointerException) t.getCause();
        else if (t.getCause() instanceof Error)
            throw (Error) t.getCause();
        else
            throw new ServiceException("Not unchecked", t);
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
	 * @param obj 对象
	 * @return 处理后字符串
	 * @author GJ
	 * @date 2011-10-26
	 */
	private Object strNull(Object obj) {// 对象为空返回空串,不为空toString
		return obj == null ? "" : obj;
	}
}
