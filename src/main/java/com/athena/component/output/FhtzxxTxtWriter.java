package com.athena.component.output;

import java.io.OutputStreamWriter;
import java.util.Map;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.db.DBOutputTxtSerivce;
import com.athena.util.exception.ServiceException;

public class FhtzxxTxtWriter extends DBOutputTxtSerivce{
	public FhtzxxTxtWriter(DataParserConfig dataParserConfig) {

	}

	public void executeOutPut(OutputStreamWriter out, Map<String,Object> rowObject) {
		try{
	        Map map=(Map) rowObject;
	        String jfdh=map.get("jfdh").toString();
	        map.put("jfdh", convert(space(jfdh,10))); //交付订单格式化
	        
	        String tch=map.get("tch").toString();
	        String tch1=convert(space(tch,15)); 
	        map.put("tch", tch1);//集装箱号格式化
	        
	        String um=map.get("um").toString();
	        map.put("um", convert(space(um,10)));//UM号格式化
	        
	        String lingjh=map.get("lingjh").toString();
	        map.put("lingjh", convert(space(lingjh,10)));//零件号格式化
        
		}catch(RuntimeException e){
            logger.error("接口" + interfaceId + "数据格式化错误", e);
            throw new ServiceException("接口" + interfaceId + "数据格式化错误", e);
		}
		super.executeOutPut(out, rowObject);
	}

	public String convert(String str){
    	
    	return str==null?"":"\""+str+"\",";
    }
	
    
    public String space(String str,int t_num){
    	int num=t_num-str.length();
    	for (int i = 0; i < num; i++) {
			str+=" "; 
		}
    	return str;
    }
    
    
	
}