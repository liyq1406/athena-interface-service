package com.athena.component.output;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.config.ExchangerConfig;
import com.athena.component.exchange.db.DBOutputTxtSerivce;
import com.athena.util.exception.ServiceException;


/**
 * 3480 ddbh下线结算
 * @author 杨志
 * @date 2016-3-8
 */

public class XiaxjsTxtWriter extends  DBOutputTxtSerivce{
	public XiaxjsTxtWriter(DataParserConfig dataParserConfig) {
	}

	/**
	 * 生成文件头 
	 *  文件头格式固定：
	 *  	DEB  HERMES  PSA       FE1172220110124003525
	 * @throws IOException 
	 */
	@Override
	public void fileBefore(OutputStreamWriter writer) {
			try {
				writer.write("DEB  ATHENA  ath7osap01"+dateToStringYMDHms());
				writer.write("\n");
			} catch (IOException e) {
	            logger.error("接口" + interfaceId + "IO输出异常", e);
	            throw new ServiceException("接口" + interfaceId + "IO输出异常", e);
			}
	}
	
	/**
	 * 为了生成文件尾部
	 * @param out
	 */
	@Override
	public void afterAllRecords(ExchangerConfig[] ecs) {
        for (ExchangerConfig ec : ecs) {
            String encoding = ec.getEncoding();
            //默认为GBK
            if(encoding==null){
                encoding = "GBK";
            }
            File file = new File(ec.getFilePath() + File.separator + ec.getFileName());
            int num =  this.readFileLineNum(encoding,file);
			if(num>0){
				num --;
			}
			OutputStreamWriter out = null;
			try {
				out = new OutputStreamWriter(new FileOutputStream(file,true),encoding);
				out.write(makefileEnd("FIN  ATHENA  ath7osap01",num));
				out.write("\n");
			} catch (UnsupportedEncodingException e1) {
				e1.printStackTrace();
			} catch (FileNotFoundException e1) {
				e1.printStackTrace();
			} catch (IOException e) {
				logger.error("接口" + interfaceId + "IO输出异常", e);
	            throw new ServiceException("接口" + interfaceId + "IO输出异常", e);
			}finally {
	            try {
	                out.close();
	            } catch (IOException e) {
	                logger.error("接口" + interfaceId + "无法关闭文件", e);
	                throw new ServiceException("接口" + interfaceId + "无法关闭文件", e);
	            }
			}
        }
        super.afterAllRecords(ecs);
	}
	
	/**
	 * 生成文件尾部
	 * 
	 * @param str
	 * @return
	 */
	public String makefileEnd(String str,int num) {
		StringBuffer sb = new StringBuffer();
		sb.append(str);
		
		String total_Str = String.valueOf(num);
		
		//String total_Str1 = String.valueOf(dataEchange.total);
		if (total_Str != null) {
			if (total_Str.length() < 9) {
				// 生成9位长的字段
				int length = 9 - total_Str.length();
				for (int i = 0; i < length; i++) {
					sb.append("0");
				}
				sb.append(total_Str);
			} else {
				// 截取后九尾
				sb.append(total_Str.substring(0, 8));
			}
		} else {
			sb.append("000000000");
		}
		return sb.toString();
	}
	
	/**
	 * 将日期转换为yyyy-MM-dd HH:mm:ss字符串
	 * @date 2012-1-18 
	 * @author hzg
	 * @param date
	 * @return String 日期字符串
	 */
	public static String dateToStringYMDHms(){
		DateFormat YMDHmsFormat = new SimpleDateFormat("yyyyMMddHHmmss",Locale.CHINA);
		return YMDHmsFormat.format(new Date());
	}

}
   