package com.athena.component.input;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.util.exception.ServiceException;


/**
 * 1080消耗点参考系 解析数据之前的处理方法
 * @date 2013-3-6
 * @author hzg
 */
public class RimaoxqmxReader extends TxtInputDBSerivce{
	private String datasourceId = "";
	protected static Logger logger = Logger.getLogger(RimaoxqmxReader.class);	//定义日志方法
	public RimaoxqmxReader(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
		datasourceId = dataParserConfig.getWriterConfig().getDatasourceId();
	}
	Map<String,String> paramclv = new HashMap<String,String>();
	
	/**
	 * 解析数据之前更新ckx_gongzsjmb表数据，set d.gongybs=0
	 */
	@Override
	public void before() {
		try{ 
			StringBuffer usercenter = new StringBuffer();
			Map<String,String> maoxqUcMap = new HashMap<String,String>();
			String inBanc = (String)dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).selectObject("inPutzbc.queryRiClvInMaoxq",paramclv);
			if(inBanc!=null && !"".equals(inBanc)){
				paramclv.put("XUQBC", inBanc);
				List<Map<String, String>> inMaoxUc = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).select("inPutzbc.queryRiClvInMaoxqUc",paramclv);
				List<Map<String, String>> ckxUc = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).select("inPutzbc.queryRiClvCkxUc",paramclv);
				for (int i = 0; i < inMaoxUc.size(); i++) {
					Map<String,String> dMap = inMaoxUc.get(i);
					String key = dMap.get("USERCENTER");
					maoxqUcMap.put(key, key);
				}
				String flag = "";
				if(inMaoxUc.size() != ckxUc.size()){
					for(Map <String, String> umap: ckxUc){
						if(maoxqUcMap.get(umap.get("USERCENTER")) ==null ){
							usercenter.append(flag).append("'").append(umap.get("USERCENTER")).append("'");
							flag= ",";
						}
					}
					if(usercenter.length()>0){
						String lastBanc = (String)dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).selectObject("inPutzbc.queryNewClvMaoxq",paramclv);
						if(lastBanc!=null && lastBanc.length()>0){
							paramclv.put("LASTBANC", lastBanc);
							paramclv.put("UC", usercenter.toString());
							dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzbc.insertCLVLastMaoxq",paramclv);
							logger.info("线程--接口" + dataParserConfig.getId() +"毛需求补全完成000");
						}
					}
				}
			}
		}catch(ServiceException e)
		{
			logger.error("线程--接口" + dataParserConfig.getId() +"毛需求补全失败"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"毛需求补全失败001"+e.getMessage());
		}		
	}

}
