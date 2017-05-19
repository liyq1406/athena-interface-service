package exchange;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.lang.StringUtils;
import org.apache.cxf.frontend.ClientProxyFactoryBean;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;
import org.junit.Test;

import com.athena.component.exchange.field.DateFieldFormat;
import com.athena.component.service.InterfaceCASCwebService;
import com.athena.component.service.bean.Baoz;
import com.athena.component.service.bean.Gysckbaoz;
import com.athena.component.service.bean.Lingjck;
import com.athena.component.service.bean.Lingjgys;
import com.athena.component.service.bean.ServiceBean;
import com.athena.component.service.imp.InterfaceServiceImp;
import com.athena.component.test.AbstractCompomentTests;
import com.athena.util.date.DateUtil;
import com.toft.core3.container.annotation.Inject;

/**
 * 接口测试类
 * @author 贺志国
 * @E-mail zghe@isoftstone.com
 * @version v1.0
 * @date 2014-3-25
 */
public class Test0226 extends AbstractCompomentTests {
	static Logger logger = Logger.getLogger(Test0226.class); 
	@Inject InterfaceServiceImp imp;
	
	//@Test
	public void test01(){
		double i_edu = 1.0;
		String str = String.valueOf(i_edu==1.0?"1":i_edu);
		assertEquals(str,"1");
		//assertEquals(str,"1.0");
	}
	
	/**
	 * OEDIPP InterfaceServiceImp 测试 
	 * @author 贺志国
	 * @date 2014-3-25
	 */
	@Test
	public void test02(){
		List<ServiceBean> list = new ArrayList<ServiceBean>();
		for(int i =1 ;i<=3;i++){
			ServiceBean bean = new ServiceBean();
			bean.setDbsqdh("17E00001");
			bean.setDbsq_date("2016-11-16");
			bean.setKjkm("12LX5053");
			bean.setCbzx("ST070");
			bean.setZzlx("97W");
			bean.setLjh(String.valueOf(987210287+i));
			bean.setSbsl(101);
			bean.setOperator("jackey");
			bean.setUsercenter("UL");
			if(i==2){
				bean.setZzlx("");
				bean.setUsercenter("UL");
			}
			if(i>2){
				bean.setUsercenter("UW");
			}
			list.add(bean);
		}
		 imp.setServiceBean(list);
		
	}
	
	
	public boolean beforeRecord(String line, String fileName, int lineNum){
		return (StringUtils.isEmpty(line))?false:true;
	}
	
	
	
	public void test03(){
		Test0226 t = new Test0226();
		double i_edu = 1.0;
		if(i_edu==1.0){
			System.out.println("right");
		}else{
			System.out.println("wrong");
		}
		String a = String.valueOf(i_edu==1.0?"1":i_edu);
		double b =2;
		System.out.println(a);
		System.out.println(b);

		//String line = "today is thuresday";
		String line = "";
		String fileName = "ath1oath3.txt";
		int lineNum = 10;
		boolean f = t.beforeRecord(line, fileName, lineNum);
		System.out.println("result====>"+f);
		
		
		if(!t.beforeRecord(line,fileName,lineNum)){
			System.out.println("in  lineNum="+lineNum);
			lineNum++;
		}
		System.out.println("out  lineNum="+lineNum);
		
		
		
		 String DH0200 = "DH0200UL5              "; //DH0200 卸货点
		 String str =DH0200.substring(6, DH0200.length()) ;
		 System.out.println("str---->"+str);
		 System.out.println("str.length---->"+str.length());
		 String newDH0200 = str.replace(str, "UL5MON");
		 System.out.println("newDH0200---->"+newDH0200);
		 while(newDH0200.length()<17){//不足17位则补足17位
			 newDH0200+=" ";
		 }
		 System.out.println("newDH0200-->"+newDH0200);
		 
		 
		 
		Format formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long startTime = System.currentTimeMillis();
		System.out.println(startTime+"开始发送文件到打印机上的开始时间:"+formatter.format(new Date()));
	}
	
	public void test04(){
		String lmp ="";
		String usercenter = "UL";
		String zongzlsh = "1E0690072";
		lmp = usercenter.concat("5L").concat(zongzlsh.substring(0, 1));
		System.out.println("lmp--->"+lmp);
	}
	
	
	public void test05(){
		String dinghcjh = "UW1";
		String dhcj = "UW5,UW3,UW2,UW1,UW6,UW9";
		int i = dhcj.indexOf("AAA");
		int b = dhcj.indexOf(dinghcjh);
		System.out.println("i===>"+i);
		System.out.println("b===>"+b);
	}
	
	/**
	 * 
	 * @author 贺志国
	 * @date 2014-8-20
	 */
	public void test06(){
		StringBuffer buf = new StringBuffer("000000"); //1720 六位规则
		String str = buf.replace(3, 3+1, "1").replace(5, 5+1, "1").toString();
		//String str1 = buf.replace(5, 5+1, "1").toString();
		System.out.println("str====>"+str);
		//System.out.println("str1====>"+str1);
		Test0226 a = new Test0226();
		Test0226 a1 = new Test0226(){
			int s1 = 88;
			int s2 = 100;
			//you can write more methods here but you will not be able to call them
			public void doSomething(){
				System.out.println("hello Something..."+s1*s2);
			}
		};
		a.x=10;
		a.test04();
		a1.x=20;
		a1.test04();
		
		System.out.println(a.x +" " + a1.x);
		a.doSomething();
		a1.doSomething();
	}
	
	
	/**
	 * 使用java正则表达式去除特殊字符
	 * @author 贺志国
	 * @date 2014-8-20
	 */
	public void test07(){
		//法文名转换 hzg 2014.8.20 
		String str = "PLAQ？UETTE-ARRET Q16 / pf 18T@uy Tête Etes-vous prêt? préparation Reculez d’un mètre";
		            //PLAQ UETTE ARRET Q16   pf 18T uy T te Etes vous pr t  pr paration Reculez d un m tre
		            //PLAQ UETTE ARRET Q16   PF 18T UY T TE ETES VOUS PR T  PR PARATION RECULEZ D UN M TRE
		             //PLAQ UETTE ARRET Q16   pf 18T uy
		String regEx="[^A-Za-z0-9]"; //匹配由数字和26个英文字母组成的字符串
		Pattern pat=Pattern.compile(regEx);
		Matcher mat=pat.matcher(str);
		String fawmc=mat.replaceAll(" "); //将除数字和26个英文字母外的字符转成空字符串
		 System.out.println("结果："+fawmc.toUpperCase());
		
		 // 只允数字  OK
		/*String str = "Zhang356Hq";
        String regEx = "[^0-9]";
        Pattern p = Pattern.compile(regEx);
        Matcher m = p.matcher(str);
        //替换与模式匹配的所有字符（即非数字的字符将被""替换）
        String ss =  m.replaceAll("").trim();
        System.out.println(ss);*/
        
        
		/*String regEx="a+"; //表示一个或多个@
		Pattern pat=Pattern.compile(regEx);
		Matcher mat=pat.matcher("@@aa@b cc@@");
		String s=mat.replaceAll("#"); 
		System.out.println(s);*/
		
	}
	
	
	int x =0;
	public void doSomething(){};
	
	
	public void test08(){
		String str ="sppv DatasourceId=4@java.lang.NoSuchMethodError: org.objectweb.asm.ClassReader.accept(Lorg/objeisitor)V";
		String []args = str.split("@");
		String aa = args[0].substring(args[0].length()-1,args[0].length());
		System.out.println(aa);
	}
	
	/**
	 * 手动拼下拉框
	 * 后台list[{YAOHLMC=表达, YAOHLLX=01}, {YAOHLMC=在途, YAOHLLX=02}, {YAOHLMC=等待交付, YAOHLLX=03}, {YAOHLMC=已交付, YAOHLLX=04}]
	 * 页面展示<t:fieldSelect property="qiandYaohlzt" caption="前段要货令状态" parents="yaohllx" parentsAlias="yaohllxParams"  
	 * src="/yaohl/selectQdYhlzt.ajax"  show="YAOHLMC" code="YAOHLLX"  width="100"></t:fieldSelect>
	 * @author 贺志国
	 * @date 2014-11-4
	 */
	//@Test
	public void test09(){
			Map<String,String>  map1 = new HashMap<String,String>();
			Map<String,String>  map2= new HashMap<String,String>();
			Map<String,String>  map3 = new HashMap<String,String>();
			Map<String,String>  map4 = new HashMap<String,String>();
			List<Map<String,String>> ztList = new ArrayList<Map<String,String>>(); 
			map1.put("YAOHLLX", "01");
			map1.put("YAOHLMC", "表达");
			map2.put("YAOHLLX", "02");
			map2.put("YAOHLMC", "在途");
			map3.put("YAOHLLX", "03");
			map3.put("YAOHLMC", "等待交付");
			map4.put("YAOHLLX", "04");
			map4.put("YAOHLMC", "已交付");
			ztList.add(map1);
			ztList.add(map2);
			ztList.add(map3);
			ztList.add(map4);
			System.out.println("ztList-->"+ztList);

		
	}
	
	private double dd;
	public double getDd() {
		return dd;
	}
	public void setDd(double dd) {
		this.dd = dd;
	}
	
	/**
	 * 去掉科学计数 转换
	 * 字符串整数超过8位变为科学计数
	 * @author 贺志国
	 * @date 2014-12-1
	 */
	//@Test
	public void test10(){
		String str = "99999999";
		this.setDd(Double.valueOf(str));
		System.out.println("double value is=>"+this.getDd());   //double value is=>9.9999999E7
		java.text.DecimalFormat df = new java.text.DecimalFormat("0");//0.00 sssss=>99999999.00
		String d = df.format(this.getDd());
		System.out.println("sssss=>"+d);//sssss=>99999999
		
	}
	
	//@Test
	public void test11(){
		String urlPath = "http://localhost:8096/athena-interface-service/services/";
		//CASC外部系统调用 hzg 2015.7.22
		ClientProxyFactoryBean factory = new ClientProxyFactoryBean();
		factory.setServiceClass(InterfaceCASCwebService.class);
		factory.setAddress(urlPath+"interfaceCASCwebService");
		System.out.println("批量CASC加载ServiceClass服务及urlPath结束");
		InterfaceCASCwebService client = (InterfaceCASCwebService) factory.create();
		System.out.println("批量CASC调用服务端开始");
		 Gysckbaoz bean = client.getLingjGysCkBaozBean("9806090180");
		System.out.println("list===>"+bean.getLingjckList().size());
		for(Lingjgys ljgys:bean.getLingjgysList()){
			System.out.println("usercenter=="+ljgys.getUsercenter());
			System.out.println("lingjbh=="+ljgys.getLingjbh());
			System.out.println("gongysbh=="+ljgys.getGongysbh());
			System.out.println("ucbzlx=="+ljgys.getUcbzlx());
		}
		for(Lingjck ljgck:bean.getLingjckList()){
			System.out.println("################################");
			System.out.println("usercenter=="+ljgck.getUsercenter());
			System.out.println("lingjbh=="+ljgck.getLingjbh());
			System.out.println("gongysbh=="+ljgck.getCangkbh());
			System.out.println("usbzlx=="+ljgck.getUsbzlx());
		}
		for(Baoz baoz:bean.getBaozList()){
			System.out.println("################################");
			System.out.println("Baozlx=="+baoz.getBaozlx());
			System.out.println("Baozmc=="+baoz.getBaozmc());
		}
		System.out.println("批量 CASC调用服务端结束");
		
	}
	
	
		public BigDecimal roundingByPackAx(BigDecimal shul,BigDecimal pack){
			//比较数量和包装容量
			if(shul.compareTo(pack) > 0){
				//数量大于包装容量,需要多个包装
				//计算数量除包装容量,需要包装个数和余数
				BigDecimal[] shuls = shul.divideAndRemainder(pack); 
				System.out.println(shuls[0]+"%%%%"+shuls[1]);
				//如果余数不为0,则表示数量无法整除,需要按包装取整
				if(shuls[1].compareTo(BigDecimal.ZERO) != 0){
					//计算取整后数量(包装个数+1)
					shul = (shuls[0].add(BigDecimal.ONE)).multiply(pack);
				}
			}else{
				//数量小于包装容量,则取整为包装容易,即一个包装
				shul = pack;
			}
			return shul;
		}
			
		 /**
		  * 输出XML文件
		  * @author 贺志国
		  * @date 2015-12-26
		  * @param filename
		  * @throws Exception
		  */
		 public void createDocumentXML(String filename) throws Exception {  
			 XMLWriter  xmlWriter = null;
			 //建立document对象，用来操作xml文件
			 Document document = DocumentHelper.createDocument();  
			 //建立根节点
			 Element testElement = document.addElement("Root");
			 testElement.addComment("This is a test for dom4j ");//加入一行注释
			 //添加一个RequestHead子节点
			 Element requestHead = testElement.addElement("RequestHead");
			 //添加一个MessageType子节点
			 Element MessageType =  requestHead.addElement("MessageType");
			 MessageType.setText("4");
			 Element SourceSystem =  requestHead.addElement("SourceSystem");
			 SourceSystem.setText("SPPV");
			 Element requestBody = testElement.addElement("requestBody");
			 Element tableElement = requestBody.addElement("table");
			 tableElement.addAttribute("id", "JHD");
			 tableElement.addAttribute("name", "供应商交货单");
			 tableElement.addAttribute("num", "100");
			 Element rowsElement = tableElement.addElement("rows");
			 Element rowElement = rowsElement.addElement("row");
			 rowElement.addAttribute("JHDH", "E05308050044");
			 rowElement.addAttribute("FHLJZL", "1");
			 rowElement.addAttribute("DDH", "C580038232");
			 rowElement.addAttribute("DDHH", "10");
			 rowElement.addAttribute("JHDHH", "23181");
			 rowElement.addAttribute("SJFHSL", "240");
			 try {
				 xmlWriter = new XMLWriter(new FileOutputStream(new File(filename)));
				 xmlWriter.write(document);
				 xmlWriter.flush();
			 } catch (Exception e) {
				 e.printStackTrace();
			 }finally {  
				 if (xmlWriter != null) {  
					 try {  
						 xmlWriter.close();  
					 } catch (IOException e) {  
					 }  
				 }  
			 }  
		 } 
		 
		 /**
		  * 日期转换
		  * @author 贺志国
		  * @date 2015-12-26
		  */
		 public void test12(){	 
			 String strValue = "Sat Dec 19 05:02:40 CST 2015";
			 SimpleDateFormat sdf=new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy", Locale.UK);
			 SimpleDateFormat sdf1=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			 String strd = "";
			 try {
				 Date sDate=sdf.parse(strValue);
				 strd = sdf1.format(sDate);
			 } catch (ParseException e) {
				 e.printStackTrace();
			 }
			 System.out.println(strd);
		 }
		 
		  /**
		   * 日期相减
		   * @author 贺志国
		   * @date 2016-3-23
		   */
		 public void test13(){
			 String beginDateStr="2016-03-23";
			 String endDateStr ="2016-03-27";
			 long day=0;
			 java.text.SimpleDateFormat format = new java.text.SimpleDateFormat("yyyy-MM-dd");    
			 java.util.Date beginDate;
			 java.util.Date endDate;
			 try
			 {
				 beginDate = format.parse(beginDateStr);
				 endDate= format.parse(endDateStr);    
				 day=(endDate.getTime()-beginDate.getTime())/(24*60*60*1000);    
				 System.out.println("相隔的天数="+day);   
			 } catch (ParseException e)
			 {
				 System.out.println("日期相减异常"+e.getMessage());
			 }   
		 }
		 
		 
		 /**
		  * 年月日时分秒转年月日
		  * @author 贺志国
		  * @date 2016-4-5
		  */
		 public void test14(){
			 java.text.SimpleDateFormat format = new java.text.SimpleDateFormat("yyyy-MM-dd");  
			 String str = "2016-1-23 1:2:22";
			 try {
				Date date = format.parse(str);
				String dstd = DateUtil.dateToStringYMD(date);
				System.out.println(dstd);
			} catch (ParseException e) {
				e.printStackTrace();
			}
			 
		 }
		 
		 /**
		  * 根据节拍计算步长
		  * @author 贺志国
		  * @date 2016-4-7
		  */
		 public void test15(){
			 String shengcjp = "16";
			 double buz = 0;  //int 改为double
			 int jiep = 0;
			 double d_buz = 0;
			 if(StringUtils.isNotEmpty(shengcjp)){
				 jiep = Integer.parseInt(shengcjp);
				 d_buz = (double)60 / jiep;
				 //BigDecimal mData = new BigDecimal(String.valueOf(d_buz)).setScale(0, BigDecimal.ROUND_HALF_UP);
				 //buz = Integer.parseInt(String.valueOf(mData));
				 System.out.println("d_buz-->"+d_buz);
				 BigDecimal mData = new BigDecimal(String.valueOf(d_buz)).setScale(2, BigDecimal.ROUND_HALF_UP);
				 buz = mData.doubleValue();
				 System.out.println("步长-->"+buz);
			 }

		 }
		 
		/**
		 *  过滤特殊字符 
		 * @author 贺志国
		 * @date 2016-9-19
		 * @param str
		 * @return
		 * @throws PatternSyntaxException
		 */
		 public  String StringFilter(String   str)   throws   PatternSyntaxException   { 
			 /**
				 * (?! #断言位置后不能匹配exp
				 * .*  #任意的字符串
				 * 
				 */
				//String regex="^(?!.*(abc)).*$";//用到了前瞻  
				//System.out.println("cba123abcsfd".matches(regex));//false不通过
				//System.out.println("cba123abscsfd".matches(regex));//true通过
			 // 只允许字母和数字       
			 // String   regEx  =  "[^a-zA-Z0-9]";                     
			 // 过滤掉所有特殊字符  
			 String regEx="[`~!@#$%^&*()+=|{}':;',\\[\\].<>/?~！@#￥%……&*（）——+|{}【】‘；：”“’。，、？]";  
			 Pattern   p   =   Pattern.compile(regEx);     
			 Matcher   m   =   p.matcher(str);     
			 return   m.replaceAll("").trim();     
		 }     
		
		/* static long sum(long a) {
			 if (a == 1) {
				 return 1;
			 } else {
				 System.out.println("Sum a="+a );//显示a的值为多少时益出
				 return a + sum(a - 1);
			 }
			
		 }
		 static long sum2(long a) {
			 if (a == 1) {
				 return 1;
			 } else {
				 System.out.println("sum2 a="+a);//同上
				 return sum2(a - 1) + a; // 仅此顺序倒了一下
			 }
		 }*/
	/**
	 * 
	 * @author 贺志国
	 * @date 2014-4-15
	 * @param args
	 */
	public static void main(String [] args){
		Test0226 t = new Test0226();
		t.test02();
		
		
	}
		 
		/* public static void main(String args[]){
			 System.out.println(sum(3750));
			 System.out.println(sum2(4200));
		 }*/
		 
		 
		
	
}
