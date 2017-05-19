package com.athena.component.input;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.util.exception.ServiceException;


/**
 * 3190 解析数据之前的处理方法
 * @date 2013-3-6
 * @author hzg
 */
public class GongzsjDBDataReader extends TxtInputDBSerivce{
	protected static Logger logger = Logger.getLogger(GongzsjZXDbDataReader.class);	//定义日志方法
	private final static int PAGES = 5000; 
	public GongzsjDBDataReader(DataParserConfig dataParserConfig) {
		super(dataParserConfig);

	}
	
	/**
	 * 解析数据之前清空ckx_gongzsjmb表数据
	 */
	@Override
	public void before() {
		try{
			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.execute("inPutddbh.gongzsjmbTempDelete");
		}catch(RuntimeException e)
		{
			logger.error("接口" + dataParserConfig.getId() +"清除ckx_gongzsjmb表时报错"+e.getMessage());
			throw new ServiceException("接口" + dataParserConfig.getId() +"清除ckx_gongzsjmb表时报错"+e.getMessage());
		}
	}
	 
	/**
	 * 行解析之前处理方法
	 */
	@Override
	public boolean beforeRecord(String line, String fileName, int lineNum){
		return (StringUtils.isEmpty(line))?false:true;
	}

	/**
	 * Merge表ckx_gongzsjmb表数据
	 */
	@Override
	public void after() {
		try{
			logger.info("接口" + interfaceId +"Merge表ckx_gongzsjmb开始");
			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.execute("inPutddbh.gongzsjmbMerge");
			
			deleteOldDate();
			logger.info("接口" + interfaceId +"Merge表ckx_gongzsjmb结束");
		}catch(RuntimeException e)
		{
			logger.error("接口" + interfaceId +"Merge表ckx_gongzsjmb表时报错"+e.getMessage());
			throw new ServiceException("接口" + interfaceId +"清除ckx_gongzsjmb表时报错"+e.getMessage());
		}
	}
	
	/**
	 * 删除掉ckx_gongzsjmb表Merge多余的数据
	 */
	public void deleteOldDate() {
		try{
			int runnum = 100;
			while(runnum>0){
				List<Map<String,String>> oldList = dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
				.select("inPutddbh.querygongzsjmbOldDate");
				logger.info("接口" + interfaceId +"MergeDelete表ckx_gongzsjmb删除:"+oldList.size());
				
				if(oldList.size()==0) {runnum=0;}
				
				int total = oldList.size();
				int pageNum = total/PAGES + (total%PAGES==0 ? 0 : 1);
	            for(int i=0;i<pageNum;i++){
	    	    	int endPage = (i+1)*PAGES;  //结束页数条数
	    	    	int startPage = i*PAGES;     //开始页数条数  
	    	    	if(i==(pageNum-1)){
	    	    		endPage = total;
	    	    	}
	    	    	List<Map<String,String>> oneList = oldList.subList(startPage, endPage);
	    	    	logger.info("接口" + interfaceId +"MergeDelete表ckx_gongzsjmb删除List:开始"+startPage+":结束"+endPage);
					dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
					.executeBatch("inPutddbh.gongzsjmbMergeDelete",oneList);	
	            }
	            runnum--;
			}

			logger.info("接口" + interfaceId +"Merge表ckx_gongzsjmb结束");
		}catch(RuntimeException e)
		{
			logger.error("接口" + dataParserConfig.getId() +"清除ckx_gongzsjmb表老数据时报错"+e.getMessage());
			throw new ServiceException("接口" + dataParserConfig.getId() +"清除ckx_gongzsjmb表老数据时报错"+e.getMessage());
		}
	}
}
