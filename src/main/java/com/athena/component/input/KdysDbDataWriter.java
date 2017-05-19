package com.athena.component.input;
import java.util.Date;


import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.util.exception.ServiceException;
import com.toft.utils.UUIDHexGenerator;

/**
 * 2060 KD件在途、物理点信息接口输入类
 * @author GJ hzg
 */
public class KdysDbDataWriter extends TxtInputDBSerivce {
	public KdysDbDataWriter(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
	}
	
	
	/**
	 * 数据解析之前清空KD件在途、物理点信息表数据
	 */
	@Override
	public void before() {
		try{
			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.execute("inPutzxc.kdwldDelete");
		}catch(RuntimeException e)
		{
			logger.error("线程--接口" + interfaceId +"清除in_kdwld表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"清除in_kdwld表时报错"+e.getMessage());
		}
	}

	/**
	 * 行记录解析之后 
	 * record 行结果记录集
	 */
	@Override
	public boolean afterRecord(Record record){
		record.put("id", UUIDHexGenerator.getInstance().generate());//ID,为UUID
		String falg=record.getString("falg");
		if("C".equals(falg)){
			String pap_sheet_id=record.getString("pap_sheet_id");
			//record.put("pap_sheet_id", "");
			record.put("kdys_sheet_id", pap_sheet_id);

			//String c_point_id = line.toString().substring(62,65);
			String c_point_id = record.getString("kdys_box_id").substring(0,3);
			record.put("c_point_id", c_point_id);  
			
			String pap_box_id=record.getString("pap_box_id");
			//record.put("pap_box_id","");
			record.put("kdys_box_id", pap_box_id);

		}	
		record.put("clzt", "0");
		record.put("cj_date", new Date());
		return true;
	}

}
