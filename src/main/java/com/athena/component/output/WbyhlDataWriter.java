package com.athena.component.output;

import java.io.OutputStreamWriter;
import java.sql.SQLException;
import java.util.Map;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.config.ExchangerConfig;
import com.athena.component.exchange.db.DBOutputTxtSerivce;
import com.athena.util.exception.ServiceException;
/**
 * 1720 外部要货令 输出接口
 * @author chenlei
 * @vesion 1.0
 * @date 2012-4-26
 */
public class WbyhlDataWriter extends DBOutputTxtSerivce {

	//缓存map集合
	@SuppressWarnings("rawtypes")
	private Map bufferMap;
	
	//创建时间 格式为:yyyymmdd HH24:Mi:ss
//	private String CJ_DATE;
	
	//计算同样零件超过第99条时将V1设置为1的一个临时变量
	private int temp_indexrow = 0;
	//相同零件计数器
	private int LJCount = 0;
	private int FirstLine = 0;
	public WbyhlDataWriter(DataParserConfig dataParserConfig) {

	}
	
	/**
	 * 执行前更新要货令状态和订单状态
	 * 1、更新订单beiz状态为F
	 * 2、更新要货令beiz1状态为F
	 */
	public void before(){
		 try{
			 dataParserConfig.getBaseDao().getSdcDataSource(sourceId).execute("outPut.updateDingdOfDingdzt");
			 dataParserConfig.getBaseDao().getSdcDataSource(sourceId).execute("outPut.updateYaohlOfBeiz1ToF");
			}catch(RuntimeException e){
				logger.error("线程--接口" + interfaceId +"before更新ck_yaohl要货令beiz1状态和xqjs_dingd订单beiz态时报错"+e.getMessage());
				throw new ServiceException("线程--接口" + interfaceId +"before更新ck_yaohl要货令beiz1状态和xqjs_dingd订单beiz状态时报错"+e.getMessage());
			}
	}
	
	
	/**
	 * 判断ckx_lingj表ANJMLXHD是否为空
	 * 目的是：如果ckx_lingj表ANJMLXHD为空，就输出一个空文件，否则输出整个文件，为了支持用户维护ckx_lingj表ANJMLXHD后可以重跑
	 * hzg 2014.5.28
	 * @throws SQLException 
	 */
	@Override
	public void outPut(ExchangerConfig write, ExchangerConfig read,OutputStreamWriter out) throws SQLException {
		try{
			String cnt = (String)dataParserConfig.getBaseDao().getSdcDataSource(sourceId)
			.selectObject("outPut.queryAnjmlxhdOfckxLingjCountIsNull");
			/**hzg 2014.5.29
			 如果第一次按件目录卸货点为空，设置了SQL为outPut.emptyOutputOf1520 ,运行完后就会一直是这个SQL
			 即使配置了按件目录卸货点，第二次运行SQL仍为outPut.emptyOutputOf1520，因此要在此处先获取原有SQL配置，最后再赋值回去*/
			String querySql = read.getSql();
			if(Integer.parseInt(cnt)>0){ //存在为空的anjmlxhd
				read.setSql("outPut.emptyOutputOf1520");
				super.outPut(write, read, out);
			}else{
				super.outPut(write, read, out);
			}  
			read.setSql(querySql);
		}catch (RuntimeException e){
			logger.error("线程--接口" + interfaceId +"零件按件目录查询出错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId + "零件按件目录查询出错"+e.getMessage());
		} 
	}
	

	/**
	 * 对数据 在数据号 和 用户中心生成 字符 
	 *  即对： SCHMD 字段重新生成
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void executeOutPut(OutputStreamWriter out, Map<String,Object> line) {
		
		if (line instanceof Map) {
			FirstLine ++;
			Map rowObject  = (Map)line;
			if(FirstLine==1){
				modify( rowObject, FirstLine) ;
				bufferMap = rowObject;
			}else if(FirstLine>0){
				//不为第一行，则将缓存数据和传进的数据比对后，写入缓存数据，并将传进数据写入缓存
				//changeBufferMap(bufferMap,rowObject,rowIndex);
				modify(bufferMap, rowObject, FirstLine) ;
				super.executeOutPut(out, bufferMap);
				bufferMap = rowObject;
			}
		}
	}

	/**
	 * 输出最后一条数据
	 * 修改 hzg 2014.5.14
	 */
//	@Override
	public void fileAfter(ExchangerConfig write, ExchangerConfig read,OutputStreamWriter out) {
		if(bufferMap != null && bufferMap.size()>0){
			//bufferMap.put("SCHMD", "000100"); //去掉14.5.14 ,不能写死
			//replace(3, bufferMap, "1");
            //hzg 2014.5.28 最后一行输出固定格式，最后三位只有两种情况：(1)000101  (2)000111  
			Map<String,String> obj = bufferMap;
			StringBuffer ss= new StringBuffer(obj.get("SCHMD"));
			obj.put("SCHMD", ss.replace(3, 4, "1").replace(5, 6, "1").toString());
			super.executeOutPut(out,bufferMap);
		}
	}

	/**
	 * 两者是否相等
	 * @param str1
	 * @param str2
	 * @return
	 */
	public boolean isEquals(String str1,String str2){
		boolean result = true;
		if(str1!=null){
			if(!str1.equals(str2)){
				//不相同
				result = false;
			}
		}else{
			//为空
			if(str2!=null){
				//两者不相同
				result  = false;
			}
		}	
			
		return result;
	}
	
	/**
	 * str1和str2不相等，就像sb写入1；相等就写入0
	 * @param sb
	 * @param str1
	 * @param str2
	 */
	public void createV(StringBuffer sb,String str1,String str2){
		if(str1!=null){
			if(str1.equals(str2)){
				//相同
				sb.append("0");
			}else{
				//不相同
				sb.append("1");
			}
		}else{
			//为空
			if(str2==null){
				//两者都为空
				sb.append("0");
			}else{
				//两者不相同
				sb.append("1");
			}
		}	
	}
	
	/**last addd method  by 王冲   2012-09-24 11:01**/
	
	
	
	public static void  main(String a[]){
		StringBuffer s= new StringBuffer("000000");
		System.out.print(s.replace(5, 6, "1")) ;
	}
	
	/**
	 * 修改V0.V1.V2.V4
	 * @param rowObject
	 * @param rowIndex
	 */
	public void modify(  Map rowObject,int rowIndex){

			
		//um ,uc 本条比较
		String um_buffer= (String) rowObject.get("UMLX");
		String uc_buffer =(String) rowObject.get("UCLX");
			
		
		//生成V0字符
		replace(0, rowObject, "1") ;
		
		//订单号或者零件号不相等，则写入1
		replace(1, rowObject, "1") ;
		
		//生成V2 没有判断 则默认生成0；
		replace(2, rowObject, "0") ;
		
		//生成V4
		if(isEquals(um_buffer, uc_buffer)){
			//相等
			replace(4, rowObject, "0") ;
			rowObject.put("UMLX", "");//如果UC型号和UA型号相同，不输出UC
			rowObject.put("FZDLC2", ""); //不输出负责代理处 92
			rowObject.put("BZJB2", "");  //不输出包装级别  3
		}else{
			//不相等
			replace(4, rowObject, "1") ;
		}
		
	}
	
	/**
	 * 修改V0.V1.V2.V4
	 * @param rowObject
	 * @param rowIndex
	 */
	@SuppressWarnings("unchecked")
	public void modify( Map bufferMap2, Map rowObject,int rowIndex){
		//得到用户中心
		String userCenter_buffer = ((String)bufferMap2.get("FSQDM")).trim();
		String userCenter_row = ((String)rowObject.get("FSQDM")).trim();
		
		//卸货点XHD
		String xhd_buffer= (String) bufferMap2.get("XHD");
		String xhd_row = (String) rowObject.get("XHD");
		
		//拿到供应商代码
		String gys_buffer  = (String) bufferMap2.get("JSQDM");
		String gys_row = (String) rowObject.get("JSQDM");
		
		//零件号
		String ljs_buffer = (String) bufferMap2.get("GMSLJH");
		String ljs_row = (String) rowObject.get("GMSLJH");
			
		//订单号
		String ddh_buffer= (String) bufferMap2.get("DDH");
		String ddh_row = (String) rowObject.get("DDH");
			
		//um ,uc 本条比较
		String um_buffer= (String) rowObject.get("UMLX");
		String uc_buffer =(String) rowObject.get("UCLX");
			
		
		//生成V0字符
		if(rowIndex == 1){
			replace(0, rowObject, "1") ;
		}else {
			//replace(0, rowObject,createV(gys_buffer,gys_row)) ;
			if(!isEquals(gys_buffer, gys_row) || !isEquals(userCenter_buffer, userCenter_row)){
				//订单号或者零件号不相等，则写入1
				replace(0, rowObject, "1") ;
			}else{
				replace(0, rowObject, "0") ;
			}
		}
		
		//生成V1字符
		if(!isEquals(ljs_buffer, ljs_row) || !isEquals(ddh_row, ddh_buffer)||!isEquals(xhd_buffer, xhd_row)){
			//订单号或者零件号不相等，则写入1
			replace(1, rowObject, "1") ;
		}else{
			replace(1, rowObject, "0") ;
		}
		
		//生成V2 没有判断 则默认生成0；
		replace(2, rowObject, "0") ;
		
		//生成V4
		if(isEquals(um_buffer, uc_buffer)){
			//相等
			replace(4, rowObject, "0") ;
			rowObject.put("UMLX", "");//如果UC型号和UA型号相同，不输出UC
			rowObject.put("FZDLC2", ""); //不输出负责代理处 92
			rowObject.put("BZJB2", "");  //不输出包装级别  3
		}else{
			//不相等
			replace(4, rowObject, "1") ;
		}
		
		//生成V3
		if(!isEquals(gys_row, gys_buffer)){ 
			//供应商不相等 将上一条数据设为1
			replace(3, bufferMap2, "1") ;
		}else{
			replace(3, bufferMap2, "0") ;
		}
		//生成V3
		//用户中心发生变化将本条设置为1，否则为0
		if(!isEquals(userCenter_row, userCenter_buffer)){
			replace(3, rowObject, "1") ;
			 //add，如果用户中心不相同，则将上一条设为1 hzg 2014.5.29
			replace(3, bufferMap2, "1") ;
		}else{
			replace(3, rowObject, "0") ;
		}
		
		//生成V5     add 如果订单号发生变化则将上一条设置为1 hzg 2014.6.12
		if(!isEquals(ljs_row, ljs_buffer)||!isEquals(xhd_row, xhd_buffer)||!isEquals(ddh_row, ddh_buffer)){
			replace(5, bufferMap2, "1") ;
			if(rowIndex == -1){
				replace(5, rowObject, "1");
			}
		}else{
			replace(5, bufferMap2, "0") ;
		}
		if(isEquals(ljs_row, ljs_buffer)){
			LJCount ++;//统计相同零件个数
		}else{
			LJCount = 0;
		}
//		//生成V5 
//		if(isEquals(ljs_row, ljs_buffer)||isEquals(xhd_row, xhd_buffer)){
//			LJCount ++;//统计相同零件个数
//			replace(5, bufferMap2, "0") ;
//		}else{
//			replace(5, bufferMap2, "1") ;  //零件号或卸货点发生变化将上一条设置为1
//			if(rowIndex==-1){
//				replace(5, rowObject, "1") ;
//			}
//			LJCount = 0;
//		}

		//V5 同样的零件，过了99条，按照零件变化进行标号 2012-12-24 hzg
		if(LJCount==99){
			replace(5, bufferMap2, "1");
			temp_indexrow = rowIndex+1;
			LJCount = 0;
		}
		//V1 相同零件超过了99条，将下一个零件设置为新零件，所以将V1设置为1  2012-12-24 hzg
		if(temp_indexrow>0&&rowIndex==temp_indexrow){
			replace(1, bufferMap2, "1");
		}
		
	}
	
	
	
	
	public  void replace(int i,Map<String,String> obj,String value){
		StringBuffer ss= new StringBuffer(obj.get("SCHMD"));
		obj.put("SCHMD", ss.replace(i, i+1, value).toString() );
	}
	
	/**
	 * str1和str2不相等，就像sb写入1；相等就写入0
	 * @param sb
	 * @param str1
	 * @param str2
	 */
	public String createV(String str1,String str2){
		if(str1!=null){
			if(str1.equals(str2)){
				//相同
				return  "0";
			}else{
				//不相同.
				return  "1";
			}
		}else{
			//为空
			if(str2==null){
				//两者都为空
				return  "0";
			}else{
				//两者不相同
				return  "1";			}
		}	
	}
}
