package com.athena.component.input;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.component.exchange.utils.ConvertUtils;
import com.athena.db.ConstantDbCode;
import com.athena.util.date.DateUtil;
import com.athena.util.exception.ServiceException;

public class WBYHLDBDataReader extends TxtInputDBSerivce{

	private String datasourceId = "";
	public WBYHLDBDataReader(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
		datasourceId = dataParserConfig.getWriterConfig().getDatasourceId();
	}
	
	/**
	 * 接口处理完成后更新CK_YAONBHL表的子仓库代码，将修改人改为interface
	 */
	@Override
	public void after() {
		try{
			//将本次接口的子仓库代码更新
			Map<String,String> params = new HashMap<String,String>();
			params.put("INTEREDITOR", "2560temp");
			Map<String,String> kehcpkshifpz= new HashMap<String,String>();
			Map<String,String> kehcpkkhtqq = new HashMap<String,String>();
			Map<String,String> kehcpkbeihtqq=new HashMap<String,String>();
			List<Map<String,String>> yaohllist = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).select("inPutzxc.yaohlList", params);
			if(yaohllist!= null && yaohllist.size()>0){
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzxc.updateNbyaohlOfZickbh",params);
				listToMap(kehcpkshifpz,dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).select("inPutzxc.kehcpkshifpz", params));
				listToMap(kehcpkkhtqq,dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).select("inPutzxc.kehcpkkehtqq", params));
				listToMap(kehcpkbeihtqq,dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).select("inPutzxc.kehcpkbeihtqq", params));
			}
			List<Map<String,String>> yaohlupdate = new ArrayList<Map<String,String>>();
			for(int i = 0;i<yaohllist.size();i++){
				Map<String,String> yaohllistMap = yaohllist.get(i);
				String yid = yaohllistMap.get("YID");
				if(kehcpkshifpz != null && kehcpkshifpz.containsKey(yid)&&kehcpkshifpz.get(yid)!= null){
					yaohllistMap.put("SHIFPZ", kehcpkshifpz.get(yid));
					if("1".equals(kehcpkshifpz.get(yid))){
						yaohllistMap.put("SHIFPZ", "1");
						yaohllistMap.put("YAOHLLB", "05");
					}else{
						yaohllistMap.put("SHIFPZ", "0");
						yaohllistMap.put("YAOHLLB", "08");
					}
				}else{
					continue;
				}
				
				String beihsjA = yaohllistMap.get("ZUIWSJ");
				if(kehcpkkhtqq != null && kehcpkkhtqq.containsKey(yid) && kehcpkkhtqq.get(yid)!= null &&!"".equals(kehcpkkhtqq.get(yid))){
					int kehtqq = Integer.parseInt(String.valueOf(kehcpkkhtqq.get(yid)));
					beihsjA = DateUtil.DateSubtractMinutes(beihsjA,kehtqq);
				}
				
				String ucck = yaohllistMap.get("USERCENTER")+yaohllistMap.get("CANGKBH");
				int kehtqq = 0;
				if(kehcpkbeihtqq != null && kehcpkbeihtqq.containsKey(ucck) && kehcpkbeihtqq.get(ucck)!= null && !"".equals(kehcpkbeihtqq.get(ucck))){
					kehtqq = Integer.parseInt(String.valueOf(kehcpkbeihtqq.get(ucck)));
				}
				params.put("kehtqq", String.valueOf(kehtqq));
				params.put("usercenter", yaohllistMap.get("USERCENTER"));
				params.put("cangkbh", yaohllistMap.get("CANGKBH"));
				params.put("juedsk", String.valueOf(beihsjA));
				String beihsj = (String)dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).selectObject("inPutzxc.queryBeihsj", params);
				beihsj = beihsj==null || "".equals(beihsj)? DateUtil.curDateTime():beihsj;
				yaohllistMap.put("BEIHSJ", beihsj);
				yaohllistMap.put("YAOHLLX", "SG");
				yaohllistMap.put("CREATOR", "2560");
				yaohllistMap.put("MUDDLX", "4");
				yaohllistMap.put("SHIFSCBHD", "1");
				yaohllistMap.put("SUODPZ", "0");
				yaohlupdate.add(yaohllistMap);
			}
			if(yaohlupdate != null && yaohlupdate.size()>0){
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).executeBatch("inPutzxc.updateyaonbhl",yaohlupdate);
			}
		}catch(RuntimeException e){
			logger.error("线程--接口" + interfaceId +"更新CK_YAONBHL表备货时间时报错"+e.getMessage(),e);
			throw new ServiceException("线程--接口" + interfaceId +"更新CK_YAONBHL表备货时间时报错"+e.getMessage());
		}
	}
	
    public String getBeihsjA(String zuiwsj,String kehtqq){
    	String beihsj = "";
    	return beihsj;
    	
    }
    
    public Map<String,String> listToMap(Map<String,String> map ,List<Map<String,String>> source){
    	for(Map<String,String> maptemp:source){
    		map.put(maptemp.get("KEY"), ConvertUtils.strNull(maptemp.get("VALUE")));
    	}
    	return map;
    }
}
