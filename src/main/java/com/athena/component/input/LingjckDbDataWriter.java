package com.athena.component.input;

import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.athena.component.DateTimeUtil;
import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.component.exchange.txt.WarmBusinessException;
import com.athena.util.exception.ServiceException;

/**
 * 2160 零件仓库设置 执行层零件仓库输入
 * @author hzg
 *
 */
public class LingjckDbDataWriter extends TxtInputDBSerivce{
	
	protected static Logger logger = Logger.getLogger(LingjckDbDataWriter.class);	//定义日志方法
	
	private Date date= new Date();
	
	public LingjckDbDataWriter(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
	}
	
	/**
	 * 行记录解析之后   给行记录增加创建人、创建时间、修改人、修改时间,并将时间类型数据格式转化成【yyyy-MM-dd HH:mm:ss】形式.
	 * */
	@Override
	public boolean afterRecord(Record record) {
		//暂时将接口传入的数据创建者取名叫temp
		record.put("CREATOR", interfaceId);
		record.put("CREATE_TIME", date);
		record.put("EDITOR", "temp");
		record.put("EDIT_TIME", date);
		try {
			record.put("shengxsj",DateTimeUtil.StringYMDToDate(record.getString("shengxsj")));
		} catch (ParseException e) {
			logger.error("线程--接口" + interfaceId + "第" + record.getLineNum() + "行数据日期转换异常  " + e.getMessage());
			throw new WarmBusinessException("日期转换错误！"+e.getMessage());
		}
		Map map = new HashMap();
		map.put("usercenter", record.getString("usercenter"));
		map.put("lingjbh", record.getString("lingjbh"));
		map.put("cangkbh", record.getString("cangkbh"));
		map.put("zickbh", record.getString("zickbh"));
		//只对存在的数据进行 子仓库变动的清理
//		List listljck = dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
//		.select("inPutzxc.lingjckQueryIsexit", map);
		//用户中心+零件编号+仓库编号+子仓库编号 来查询，如果记录存在，则表示子仓库没变化。如果记录不存在，表示子仓库有变化，清空定置库位。
		Integer lj = (Integer)dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
		.selectObject("inPutzxc.lingjckQueryIsexits", map);
		if(lj==0){
			//更新定置库位的置为空
			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.execute("inPutzxc.lingjckUpdateDingzkw", map);
		}
		return true;
	}
	
	/**
	 * mantis 0004753
	 */
	public void finishAfter() {
		//删除本次接口传递以外的数据
		updateTable("inPutzxc.lingjckDelete");
		//将剩余数据的修改人改为interface
		updateTable("inPutzxc.lingjckUpdate");
	}
	
	/**
	 * 更新表信息
	 * @param String sqlmapId
	 */
	private void updateTable(String sqlmapId) {
		try{
			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.execute(sqlmapId);
		}catch(RuntimeException e)
		{
			logger.error("线程--接口" + interfaceId +"更新ckx_lingjck表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"更新ckx_lingjck表时报错"+e.getMessage());
		}
	}
}
