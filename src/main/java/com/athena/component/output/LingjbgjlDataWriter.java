package com.athena.component.output;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.db.DBOutputTxtSerivce;

/**
 * 1450 变更记录
 * @author wy
 * @version v1.0
 * @date 2014-2-27
 */
public class LingjbgjlDataWriter extends DBOutputTxtSerivce {
	private static Logger logger = Logger.getLogger(LingjbgjlDataWriter.class);
	public LingjbgjlDataWriter(DataParserConfig dataParserConfig) {

	}
	
	/**
	 * 1450输出前,对数据进行判断来截取
	 */
	
	public void  beforeRecords(List<Map<String,String>> sourcelist){
		System.out.println(sourcelist.size());
		for (int i = sourcelist.size()-1; i>=0; i--) {
			//先获取 用户中心\零件编号\现编号 根据这3个条件去关联物流路径去查询记录是否存在
			String usercenter = sourcelist.get(i).get("USERCENTER");
			String lingjbh = sourcelist.get(i).get("LINGJBH");
			String xianbh = sourcelist.get(i).get("XIANBH");
			Map map = new HashMap();
			map.put("usercenter", usercenter);
			map.put("lingjbh", lingjbh);
			map.put("xianbh", xianbh);
			//根据现编号的长度来看 是仓库的替换 或 消耗点的替换
			if(xianbh.length()==3){
				//仓库的替换 先是根据 目的地去取 外部模式 
				List<Map<String,String>> muddlist = baseDao.getSdcDataSource(sourceId).select("outPut.mosQueryByMudd", map);
				//如果根据目的地取到的外部模式记录存在的话
				if(muddlist.size()!=0){
					for (Map<String, String> mapmudd : muddlist) {
						String waibms = mapmudd.get("WAIBMS");
						   if("R".equals(waibms.substring(0, 1)) && !"1".equals(sourcelist.get(i).get("SHIFSY"))){
							   sourcelist.remove(i);
						   }
					}
				}else{
						logger.info("用户中心 "+ usercenter + "零件编号 " +lingjbh+ "现编号 "+xianbh+" 关联物流路径目的地对应的外部模式为空");
					//当目的地对应的外部模式不存在的时候 ,需要根据 线边库 去取 对应的模式2
					List<Map<String,String>> xianbcklist = baseDao.getSdcDataSource(sourceId).select("outPut.mosQueryByXianbck", map);
					if(xianbcklist.size()!=0){
						for (Map<String, String> mapxianbck : xianbcklist) {
							String mos2 = mapxianbck.get("MOS2");
							   if("R".equals(mos2.substring(0, 1)) && !"1".equals(sourcelist.get(i).get("SHIFSY"))){
								   sourcelist.remove(i);
							   }
						}
					}else{
						logger.info("用户中心 "+ usercenter + "零件编号 " +lingjbh+ "现编号 "+xianbh+" 关联物流路径线边仓库对应的模式2为空");
						sourcelist.remove(i);
					}
				}
			}else{
				//消耗点的替换
				List<Map<String,String>> xhdlist = baseDao.getSdcDataSource(sourceId).select("outPut.mosQueryByxhd", map);
				if(xhdlist.size()!=0){
					for (Map<String, String> mapxhd : xhdlist) {
						   String mos = mapxhd.get("MOS");
						   if("R".equals(mos.substring(0, 1)) && !"1".equals(sourcelist.get(i).get("SHIFSY"))){
							   sourcelist.remove(i);
						   }
					}
				}else{
					 logger.info("用户中心 "+ usercenter + "零件编号 " +lingjbh+ "现编号 "+xianbh+" 关联物流路径分配循环对应的模式为空");
					 sourcelist.remove(i);
				}
			}
		}

    }
	
}
