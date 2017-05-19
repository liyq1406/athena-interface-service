package com.athena.component.output;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.athena.component.exchange.FileLog;
import com.athena.component.exchange.InziDbUtils;
import com.athena.component.exchange.OutputDataService;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.config.DataParserConfigFactory;
import com.athena.component.exchange.config.DataParserXmlHelper;
import com.athena.component.exchange.config.ExchangerConfig;
import com.athena.component.exchange.db.DBOutputTxtSerivce;
import com.athena.component.exchange.field.DataField;
import com.athena.component.exchange.utils.FileUtis;
import com.athena.component.service.bean.InterfaceConfig;
import com.athena.component.service.bean.OrderConfig;
import com.athena.component.service.utls.InfaceParserConfig;
import com.athena.util.exception.ServiceException;
import com.toft.core3.container.annotation.Component;
import com.toft.core3.container.annotation.Inject;
import com.toft.core3.ibatis.support.AbstractIBatisDao;
import com.toft.utils.UUIDHexGenerator;

/**
 * 多个接同时输出
 * 用于同时输出多个接口配置文件，如果其中一个接口报错就抛1
 * @date 2013-7-23
 * @author hzg
 *
 */
@Component
public class DuoJieKouOutputWriter implements OutputDataService{
    protected final Logger logger=Logger.getLogger(DuoJieKouOutputWriter.class);//定义日志方法
    @Inject
    protected AbstractIBatisDao baseDao;//注入baseDao

    protected DataParserConfig dataParserConfig;

    private static final String AFTER = "after";
    
    private static final String LineSign = "\\n"; //换行符

    private final static int PAGESIZE = 10000; //每次固定输入10000条(可配置，默认为10000)
    
//    protected DataField[] dataFields;

    protected String sourceId = ""; //数据源ID
    protected String interfaceId = ""; //数据源ID
    
    StringBuffer fileNameAll = new StringBuffer();
    
    Map<String,String> filePathMap = new HashMap<String,String>();
    
    Set<String> fpath = new HashSet<String>();

    protected  int total=0; //处理总记录数
    //接口编号
//    protected String interfaceId; 
    //文件运行时间
//    protected String file_begintime;
    
    public boolean  write(String codeId) throws ServiceException {
    	boolean flag = false;
    	List<String> jkList  =  getComplexJkh(codeId);
    	//循环数组，输出接口文件
    	for(String jkh : jkList){
    		//子接口运行开始时间 
    		String nowTime = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
    		//初始化配置  
    		DataParserConfig dataParserConfig = initDataParserConfig(jkh);
    		this.dataParserConfig = dataParserConfig;
    		before();
    		//解析单个接口的配置文件并输出接口文件
    		flag = paseDataExchanger(dataParserConfig,jkh);
    		after();
    		//更新子接口in_zidb时间与状态
    		InziDbUtils.getInstance().UpdateInZIDB_ZTByMx(nowTime, baseDao.getConnection(), jkh);
			InziDbUtils.getInstance().getZIDB_ZTByid(baseDao.getConnection(),jkh);
    	}
    	afterAllRecords(codeId);
    	return flag;
    }
    
    
    /**
     * 解析接口配置
     * @author Hezg
     * @date 2013-7-25
     * @param dataParserConfig
     * @param jkh 
     * @return
     */
    public boolean paseDataExchanger(DataParserConfig dataParserConfig,String jkh){
    	boolean flag = false;
    	StringBuffer fileName = new StringBuffer();
    	String file_begintime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
    	try{
    		ExchangerConfig readerConfig = dataParserConfig.getReaderConfig(); //得到reader的属性值
    		sourceId = readerConfig.getDatasourceId();
    		createOutputClass(jkh);
//    		this.dataFields = dataParserConfig.getDataFields(); // 得到配置文件字段
    		interfaceId = dataParserConfig.getId();
    		ExchangerConfig[] ecs = dataParserConfig.getWriterConfigs();
    		//处理要输出的的字符信息
            DataField[] dataFile = dataParserConfig.getDataFields();
    		for (ExchangerConfig ec : ecs) {
    			outPut(ec, readerConfig, createOut(ec),dataFile);
    			fileName.append(ec.getFileName()).append(","); //记录日志表文件名
    			//将所有的输出文件放到StringBuffer中，路径放到Map中
    			fileNameAll.append(ec.getFileName()).append(",");
    			filePathMap.put("filePath", ec.getFilePath());
    			filePathMap.put("athfilePath", ec.getAthfilePath());
    		}
    		flag = true;
    	} catch (SQLException sqlEx){
    		logger.error(sqlEx.getMessage());
    		throw new ServiceException(sqlEx);
    	} catch(RuntimeException ex){
    		logger.error(ex.getMessage());
    		throw new ServiceException(ex);
    	}finally{
    		logger.info("接口" + dataParserConfig.getId() + "结束输出");
    		file_info(fileName.toString(),flag,jkh,file_begintime);

    	}
    	return flag;
    }
    
    
    /**
     * 判断是否有继承类存在，存在则执行相关方法调用
     * @author Hezg
     * @param jkh 接口号
     * @date 2013-8-5
     */
    public void createOutputClass(String jkh){
		String readerClass = dataParserConfig.getReaderConfig().getExchangerClass();
    	if(StringUtils.isNotEmpty(readerClass)){
    		if("2120".equals(jkh)){
    			KuckzTxtWriter kuckz = new KuckzTxtWriter();
				kuckz.beforeExecute2120(baseDao,sourceId);
			}else if("2790".equals(jkh)){
					LingjckHZDataWriter lingjck = new LingjckHZDataWriter();
					lingjck.beforeExecute(baseDao,sourceId,"2790");
			}else if("2740".equals(jkh)){
				LingjxhdHZDataWriter lingjxhd = new LingjxhdHZDataWriter();
				lingjxhd.beforeExecute(baseDao,sourceId,"2740");
			}else if("2130".equals(jkh)){
				YicsblingjxhTxtWriter yicsb = new YicsblingjxhTxtWriter();
				yicsb.beforeExecuteYicsb(baseDao,sourceId);
			}
		}
    }
    
 
    /**
     * 存放大接口下的所有子接口号
     * @author Hezg
     * @date 2013-7-31
     * @param configId
     * @return 子接口数组
     */
    protected List<String> getComplexJkh(String configId){
    	//1：解析 enchange-interface.xml 得到此taskName为id的配置信息
		InterfaceConfig inter = InfaceParserConfig.getInstance().getDataParserConfig(configId);
		List<String> jkList = new ArrayList<String>();
		if (inter != null) {
			//子接口集合
			List<OrderConfig> orders = inter.getOrders();
			//获取所有子接口编号
			for (OrderConfig order : orders) {
				jkList.add(order.getId());
			}
		}
		return jkList;
    }
    /**
     * 初始化配置
     * @param configId  接口ID
     * @return
     */
    private DataParserConfig initDataParserConfig(String configId) {
        DataParserConfig dataParserConfig =
                DataParserConfigFactory.getInstance().getDataParserConfig(configId);
        dataParserConfig.setId(configId);
        dataParserConfig.setBaseDao(baseDao); //设置dao类
        if("txt".equals(dataParserConfig.getGroupConfig().getWriter())){
            //为了直接从接口总表拿出用户中心，及所生成的文件
            List<String> list = InziDbUtils.getInstance().selectById(dataParserConfig.getReaderConfig().getDatasourceId(), configId,"toTxt");
            if(list!=null){
                //创建多个ExchangerConfig
                List<ExchangerConfig> wclist = new ArrayList<ExchangerConfig>();
                for(String usercenter : list){
                    wclist.add(DataParserXmlHelper.parseWriterExchangerConfig(dataParserConfig.getWriterConfigs()[0], usercenter));
                }
                dataParserConfig.setWriterConfigs(wclist.toArray(new ExchangerConfig[wclist.size()]));
            }
        }
        return dataParserConfig;
    }
    
    
    /**
	 * 记录文件日志
	 * @author Hezg
	 * @date 2013-2-4
	 * @param wenjmc 文件名称
	 * @param file_satus 运行状态        1 成功(已执行)  -1 失败(已执行)
	 * @return 
	 */
    protected void file_info(String wenjmc,boolean file_satus,String interfaceId,String file_begintime) {
        AtomicInteger insert_num = new AtomicInteger();
        AtomicInteger update_num = new AtomicInteger();
        AtomicInteger error_num = new AtomicInteger();
		Map<String,String> params = new HashMap<String,String>();
		params.put("SID", UUIDHexGenerator.getInstance().generate());
		params.put("INBH", interfaceId);
		params.put("fileName", wenjmc);
		params.put("file_begintime", file_begintime);
		params.put("file_endtime", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
		params.put("insert_num", String .valueOf(insert_num));
		params.put("update_num", String .valueOf(update_num));
		params.put("error_num", String .valueOf(error_num));
		params.put("file_satus", file_satus?"1":"-1");
		
		FileLog.getInstance(sourceId).insert_file_info(params,baseDao);
	}
    
    /**
     * 执行文件输出
     * @author Hezg
     * @date 2013-7-25
     * @param write
     * @param read
     * @param out
     * @throws SQLException
     */
    protected void outPut(ExchangerConfig write, ExchangerConfig read,OutputStreamWriter out,DataField[] dataFile) throws SQLException{
        int totalPage = getTotalPage(write.getUsercenter(), read.getSql(),read.getIsAllSet());
        try {
            for(int i=0;i<totalPage;i++){
                outPutTxt(i,write,read, out,dataFile);
            }
        } finally {
            try {
                out.close();
            } catch (IOException e) {
                logger.error("接口" + dataParserConfig.getId() + "无法关闭文件", e);
            }
        }
    }
    
    /**
    *
    * @param pageNum
    * @param write
    * @param read
    * @param out
    * @throws SQLException
    */
	@SuppressWarnings("unchecked")
	private void outPutTxt(int pageNum,ExchangerConfig write,ExchangerConfig  read, OutputStreamWriter out,DataField[] dataFile) throws SQLException{
   	int endPage = (pageNum+1)*PAGESIZE;  //结束页数条数
   	int startPage = pageNum*PAGESIZE+1;     //开始页数条数
       String head = "select * from ("; 
       String foot =" and rownum <= "+endPage+") t"
       			+" where t.RN >= "+startPage ;
       Map<String,String> params = putParams(write.getUsercenter(),head,foot.toString(),read.getIsAllSet()); //分页参数替换
       List<Map<String,String>> dataList = baseDao.getSdcDataSource(sourceId).select(read.getSql(), params);
       txtOutPut(dataList, out,dataFile); //直接输出为txt
   }
    
	 /**
     * 直接输出为TXT文本 
     * @param list 输出数据集
     */
    private void txtOutPut(List<Map<String,String>> list,OutputStreamWriter out,DataField[] dataFile){
		List<Map<String,Object>> outPutlist = getDoWriterLine(list,dataFile);
		beforeAllRecordsSave(list,outPutlist);
        for(int i=0;i<outPutlist.size();i++){
        	executeOutPut(out, outPutlist.get(i),dataFile);
        }
    }
    
    public void  beforeAllRecordsSave(List<Map<String,String>> sourcelist,List<Map<String,Object>> outPutlist){
    	int num = outPutlist == null? 0 : outPutlist.size();
    	this.setTotal(num);
    }
    
    /**
     * 将数据按格式输出到文本
     * @param out 输出流
     * @param rowObject 
     */
    @SuppressWarnings("rawtypes")
	public void executeOutPut(OutputStreamWriter out, Map<String,Object> rowObject,DataField[] df) {
        Map rowObjectMap = (Map) rowObject;
        StringBuffer sb = new StringBuffer();

        //处理要输出的的字符信息
//        DataField[] df = dataParserConfig.getDataFields();
        for(int i=0;i<df.length;i++){
            String writerColumn = df[i].getWriterColumn(); //列名
            int length = df[i].getLength(); //列长度
            String separate = df[i].getSeparate(); //分隔符
            String separate_size = df[i].getSeparate_size(); //分隔符的位置

            //作为字符串处理--- null
            String columnValue;
            Object columnObject = rowObjectMap.get(writerColumn);
            columnValue = columnObject!=null?(columnObject.toString()): "";

            //创建一个字段
            createField(sb,columnValue,length,separate,separate_size);
        }

        //生成换行符
        if(sb.length()>0){
            sb.append("\n");
        }

        //out对象输出
        try {
            out.write(sb.toString());
            out.flush();
        } catch (IOException e) {
            logger.error("接口" + interfaceId + "IO输出异常", e);
            throw new ServiceException("接口" + interfaceId + "IO输出异常", e);
        }

    }
    
    /**
     * 生成一个字段
     * 	1：如果columnValue为空，则输出length制定长度的字符；
     * 	2：如果columenValue不为空，则输出制定长度的字符；不足长度的补separate；
     * 	3：默认separate是空格；separate_size是 after;
     * @param sb
     * @param columnValue
     * @param length  字段输出的长度
     * @param separate
     * @param separate_size 
     */
    private void createField(StringBuffer sb, String columnValue, int length,
                             String separate, String separate_size) {
        if(columnValue!=null){
            //输出字段不为空
            String charset = dataParserConfig.getWriterConfigs()[0].getEncoding();
            int value_length = 0;
            try {
                value_length = columnValue.getBytes(charset).length;
            } catch (UnsupportedEncodingException e) {
                logger.error("接口" + interfaceId + "创建字符串不支持编码格式为" + charset, e);
            }
            if(length>0){
                if(value_length>=length){
                    //字段长度比要输出的长度长 则从左边截取
                    sb.append(makeStrByLength(columnValue,length,charset));
                }else{
                    if(separate_size!=null){
                        //填充符位置不为空
                        if(separate!=null){
                            //填写了 分隔符
                            fillSeparate(length-value_length,separate_size,sb,separate,columnValue);
                        }else{
                            //没有填写分隔符  则默认补空格
                            fillSeparate(length-value_length, separate_size, sb, " ", columnValue);
                        }
                    }else{
                        //填充符位置为空 默认在后面添加
                        if(separate!=null){
                            //填写了 分隔符
                            fillSeparate(length-value_length,AFTER,sb,separate,columnValue);
                        }else{
                            //没有填写分隔符  则默认补空格
                            fillSeparate(length-value_length, AFTER, sb, " ", columnValue);
                        }
                    }
                }
            }else{
                //没有填写长度  就默认输出 columnValue
                sb.append(columnValue);
            }
        }else{
            //输出字段为空
            if(length>0){
                if(separate!=null){
                    //填充符不为空
                    for(int i=0;i<length;i++){
                        sb.append(separate);
                    }
                }else{
                    //填充符为空 默认用空格填充
                    for(int i=0;i<length;i++){
                        sb.append(" ");
                    }
                }
            }
        }

    }
    
    
    /**
     * 如果数据库取出的字段比要生成的字段长，则按照字节来截取
     * @param columnValue
     * @param length
     * @param charset
     * @return
     */
    private String makeStrByLength(String columnValue,int length,String charset){
        String result = null;
        try {
            byte[] bys = columnValue.getBytes(charset);
            byte[] bs = new byte[length];
            for(int i=0;i<length;i++){
                bs[i] = bys[i];
            }
            result = new String(bs,charset);
        } catch (UnsupportedEncodingException e) {
            logger.error("接口" + interfaceId + "取出数据库字符串不支持编码格式为" + charset, e);
        }


        return result;
    }

    /**
     * 填写分隔符
     * @param i 填写的位数
     * @param separate_size 分隔符位置
     * @param sb
     * @param separate  分隔符
     * @param columnValue  此字段的值
     */
    private void fillSeparate(int i, String separate_size, StringBuffer sb,
                              String separate,String columnValue) {

        if(AFTER.equals(separate_size)){
            //在后面添加
            sb.append(columnValue);
            for(int j=0;j<i;j++){
                //为了支持换行
                if(LineSign.equals(separate)){
                    sb.append("\n");
                    break;
                }else{
                    sb.append(separate);
                }
            }
        }else{
            //在前面添加
            for(int j=0;j<i;j++){
                //为了支持换行
                if(LineSign.equals(separate)){
                    sb.append("\n");
                    break;
                }else{
                    sb.append(separate);
                }
            }
            sb.append(columnValue);
        }

    }
    
	
    /**
     * 将数据集转换为配置的字段集
     * @param dataList
     * @return
     */
    private List<Map<String,Object>> getDoWriterLine( List<Map<String,String>> dataList,DataField[] dataFields){
    	List<Map<String,Object>> outPutlist =  new ArrayList<Map<String,Object>>(); //将输出的数据
        for(int i=0;i<dataList.size();i++){
            Map<String,Object> result = new HashMap<String,Object>();
            for(DataField dataField:dataFields){
                Object value = dataList.get(i).get(dataField.getReaderColumn().toUpperCase());
                if(value!=null&&dataField.getWriterColumn()!=null){
                    result.put(dataField.getWriterColumn(), value);
                }
            }
            outPutlist.add(result);
        }
        return outPutlist;
    }
    
    /**`
     * 计算分多少页
     */
    public int getTotalPage(String usercenter, String sql,String flagStr) throws SQLException{
        int totalNum = getTotalNum(usercenter, sql,flagStr);
        int totalPage  = totalNum/PAGESIZE + (totalNum%PAGESIZE==0 ? 0 : 1);
        return totalPage;
    }
    
    /**
    * 获取数据库将输出总记录数
    * @param usercenter 用户中心
    * @param sqlId ibatis sql id
    * @param flagStr 标识
    * @return 记录条数
    * @throws SQLException
    */
	@SuppressWarnings("rawtypes")
	private int getTotalNum(String usercenter,String sqlId,String flagStr) throws SQLException{
   	int countNum = 0;
       String head = "select count(1) COUNTNUM from (";
       String foot = " ";
       Map<String,String> params = putParams(usercenter, head,foot.toString(),flagStr); //分页参数替换
       Map dataMap= (Map)baseDao.getSdcDataSource(sourceId).selectObject(sqlId, params);
       countNum = Integer.parseInt(dataMap.get("COUNTNUM").toString());
       return countNum;
   }
    
	  /**
     * 参数替换
     * @param head 开头参数
     * @param foot 结尾参数
     * @param flagStr
     * @return
     */
    private Map<String,String> putParams(String usercenter, String head,String foot,String flagStr){
    	Map<String,String> params = new HashMap<String,String>();
    	String temp_foot = "".equals(foot.trim())? ")": foot;
    	String tempStr = (head.indexOf("COUNTNUM")!= -1)?" )" :" ";
    	String strmiddle = "false".equals(flagStr)? " "+makeIncrementSql()+" ":" ";
    	String strfoot = "false".equals(flagStr)? " "+foot + tempStr: temp_foot;
        if (!usercenter.equals("ALL")) {
            params.put("usercenter", this.getUsercenter(usercenter));
        }
        params.put("middle", strmiddle);
        params.put("head", head);
        params.put("foot", strfoot);
    	return params;
    }
    
    /**
     * 生成所需查询的用户中心
     * @param user
     * @return
     */
	private String getUsercenter(String user){
		String[] str = user.split(",");
		String userCenter = "";
		for(int i=0;i<str.length;i++){
			if(i==str.length-1){
				userCenter +="'"+str[i]+"'";
			}else{
				userCenter +="'"+str[i]+"',";
			}
		}
		return userCenter;
    }
    
    /**
     * 生成带有增量条件的sql
     * 	 and to_char(edit_time, 'yyyymmddHH24MIss') between
     (SELECT to_char(i.lastcpltime, 'yyyymmddHH24MIss')
     FROM IN_ZIDB i
     WHERE i.inbh = 'id1') and to_char(sysdate, 'yyyymmddHH24MIss')
     * @return
     */
    protected String makeIncrementSql() {
        //1：查找接口总表数据库,拿出此接口的 完成时间
        //2: 以此完成时间，和当前时间做条件 生成串
        //3: 更新此接口总表 此接口的完成时间，上上次完成时间
        StringBuffer sb = new StringBuffer();
        String nowTime = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        sb.append(" AND TO_CHAR(EDIT_TIME, 'yyyymmddHH24MIss') BETWEEN ");
        sb.append(" (SELECT TO_CHAR(i.LASTCPLTIME, 'yyyymmddHH24MIss') ");
        sb.append(" FROM IN_ZIDB i ");
        sb.append(" WHERE i.INBH = '"+dataParserConfig.getId()+"') and '"+nowTime+"' ");

        return sb.toString();
    }
    
    /**
     * 创建输出流
     * @param ec 配置文件对象
     * @return
     */
    private OutputStreamWriter createOut(ExchangerConfig ec) {

        String filePath = ec.getFilePath();
        String fileName = ec.getFileName();
        String encoding = ec.getEncoding();

        //默认为GBK
        if(encoding==null){
            encoding = "GBK";
        }
        //System.out.println("filePath:"+filePath+"---"+"fileName:"+fileName+"---"+"encoding:"+encoding);
        //1：如果没有用户输入的路径 就创建此路径
        File file = new File(filePath);
        if(!file.exists()){
            file.mkdirs();
        }

        //2:生成输出文件路径
        String outName = createRealPath(filePath,fileName);

        //3:创建输出流
        OutputStreamWriter writer = null;
        try {
        	if("true".equals(ec.getIsGoOnOut())){
                writer = new OutputStreamWriter(new FileOutputStream(new File(outName),true),encoding);
        	}else{
                writer = new OutputStreamWriter(new FileOutputStream(new File(outName)),encoding);	
        	}
        } catch (UnsupportedEncodingException e) {
            logger.error("接口" + interfaceId + "不支持编码格式为" + encoding, e);
            throw new ServiceException("接口" + interfaceId + "不支持编码格式为", e);
        } catch (FileNotFoundException e) {
            logger.error("接口" + sourceId + "没找到文件" + outName, e);
            throw new ServiceException("接口" + interfaceId + "没找到文件" + outName, e);
        }

        return writer;
    }
    
    /**
     * 生成输出文件路径
     * @param filePath 文件路径
     * @param fileName 文件名称
     * @return String 文件路径+文件名称 
     */
    private String createRealPath(String filePath, String fileName) {
        StringBuffer sb = new StringBuffer();
        sb.append(filePath);
        sb.append(File.separator);
        sb.append(fileName);
        return sb.toString();
    }
    
    /**
     * 所有接口都完成输出到athena文件夹之后将输出文件移到tmp文件夹中
     * @author Hezg
     * @date 2013-7-25
     * @param ecs
     */
    public void  afterAllRecords(String headJkh){
    	String [] fileNameStr = (fileNameAll.toString().substring(0, fileNameAll.toString().length()-1)).split(",");
    	String encoding = "GBK";
    	//循环接口文件并转移
    	for(String fname : fileNameStr){
    		 String outPutfileName = filePathMap.get("athfilePath") + File.separator +fname;
             File newfilepath = new File(filePathMap.get("athfilePath"));
             if(!newfilepath.exists()){
             	newfilepath.mkdirs();
             }
             moveOutFile(encoding, filePathMap.get("filePath") + File.separator + fname,outPutfileName,headJkh);
    	}
    }
    
    /**
     * 读取文件行数据
     * @param file
     */
    public void moveOutFile(String encoding,String oldfileName,String outPutfileName,String interfaceId) {
        InputStream inStream = null;
        FileOutputStream fs = null;
        try {   
            int bytesum = 0;   
            int byteread = 0;   
            File oldfile = new File(oldfileName);   
            if (oldfile.exists()) { // 文件存在时    
            	inStream = new FileInputStream(oldfileName); // 读入原文件   
            	fs = new FileOutputStream(outPutfileName); 
                byte[] buffer = new byte[1444];   
                while ((byteread = inStream.read(buffer)) != -1) {   
                    bytesum += byteread; // 字节数 文件大小    
                    fs.write(buffer, 0, byteread);   
                }   
            }   
        } catch (Exception e) {   
            logger.error("接口" + interfaceId + "移动文件错误" + oldfileName, e);
            throw new ServiceException("接口" + interfaceId + "移动文件错误" + oldfileName, e);
        }finally {
            try {
            	inStream.close();
            	fs.close();
            } catch (IOException e) {
                logger.error("接口" + interfaceId + "无法关闭文件", e);
                throw new ServiceException("接口" + interfaceId + "无法关闭文件", e);
            }
        }   
    }
    
   /**
    * 执行前调用方法
    * @author Hezg
    * @date 2013-7-31
    */
    public void before(){

    }
    
    /**
     * 执行后调用方法
     * @author Hezg
     * @date 2013-7-31
     */
    public void after(){
    	
    }
    
    public int getTotal() {
		return total;
	}

	public void setTotal(int total) {
		this.total = total;
	}


	@Override
	public boolean write(DataParserConfig dataParserConfig)
			throws ServiceException {
		// TODO Auto-generated method stub
		return false;
	}


	@Override
	public void afterAllRecords(ExchangerConfig[] ecs) {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void fileAfter(ExchangerConfig write, ExchangerConfig read,
			OutputStreamWriter out) {
		// TODO Auto-generated method stub
		
	}
    
}
