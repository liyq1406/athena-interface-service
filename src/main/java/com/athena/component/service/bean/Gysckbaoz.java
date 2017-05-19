package com.athena.component.service.bean;

import java.math.BigDecimal;
import java.util.List;

/**
 * 
 * @author 贺志国
 * @E-mail zghe@isoftstone.com
 * @version v1.0
 * @date 2015-7-23
 */
public class Gysckbaoz {
	private String usercenter; //用户中心
	private String lingjbh;   //零件编号
	private String gongysbh;  //零件供应商
	private String cangkbh;  //仓库编号
	private String ucbzlx;  //UC包装类型
	private String uclx;  //UC类型
	private BigDecimal ucrl;  //UC容量
	private BigDecimal uaucgs;  //UA中UC个数
	private String uabzlx;  //UA包装类型
	private BigDecimal uarl;  //UA容量
	private String usbzlx;  //US包装类型
	private BigDecimal usbzrl;  //US包装容量
	private String gaib;  //盖板
	private String neic;  //内衬
	private String baozlx;  //包装类型
	private String baozmc;  //包装名称
	private String changd;  //长度（毫米）
	private String kuand;  //宽度
	private String gaod;  //高度
	private String baozzl; //包装重量（千克）
	private String leib;  //类别
	private String caiz;  //材质
	private String shifhs;//是否回收
	private String zhedgd;//折叠高度（毫米）
	private String duidcs;//堆垛层数
	private String baiffx;//摆放方向
	
	private List<Lingjgys> lingjgysList;  //零件供应商集合
	private List<Lingjck> lingjckList;   //零件仓库集合
	private List<Baoz> baozList;  //包装集合
	
	
	
	public List<Lingjgys> getLingjgysList() {
		return lingjgysList;
	}
	public void setLingjgysList(List<Lingjgys> lingjgysList) {
		this.lingjgysList = lingjgysList;
	}
	public List<Lingjck> getLingjckList() {
		return lingjckList;
	}
	public void setLingjckList(List<Lingjck> lingjckList) {
		this.lingjckList = lingjckList;
	}
	public List<Baoz> getBaozList() {
		return baozList;
	}
	public void setBaozList(List<Baoz> baozList) {
		this.baozList = baozList;
	}
	public String getUsercenter() {
		return usercenter;
	}
	public void setUsercenter(String usercenter) {
		this.usercenter = usercenter;
	}
	public String getLingjbh() {
		return lingjbh;
	}
	public void setLingjbh(String lingjbh) {
		this.lingjbh = lingjbh;
	}
	public String getGongysbh() {
		return gongysbh;
	}
	public void setGongysbh(String gongysbh) {
		this.gongysbh = gongysbh;
	}
	public String getCangkbh() {
		return cangkbh;
	}
	public void setCangkbh(String cangkbh) {
		this.cangkbh = cangkbh;
	}
	public String getUcbzlx() {
		return ucbzlx;
	}
	public void setUcbzlx(String ucbzlx) {
		this.ucbzlx = ucbzlx;
	}
	public String getUclx() {
		return uclx;
	}
	public void setUclx(String uclx) {
		this.uclx = uclx;
	}
	public BigDecimal getUcrl() {
		return ucrl;
	}
	public void setUcrl(BigDecimal ucrl) {
		this.ucrl = ucrl;
	}
	public BigDecimal getUarl() {
		return uarl;
	}
	public void setUarl(BigDecimal uarl) {
		this.uarl = uarl;
	}
	public BigDecimal getUaucgs() {
		return uaucgs;
	}
	public void setUaucgs(BigDecimal uaucgs) {
		this.uaucgs = uaucgs;
	}
	public BigDecimal getUsbzrl() {
		return usbzrl;
	}
	public void setUsbzrl(BigDecimal usbzrl) {
		this.usbzrl = usbzrl;
	}
	public String getUabzlx() {
		return uabzlx;
	}
	public void setUabzlx(String uabzlx) {
		this.uabzlx = uabzlx;
	}
	public String getUsbzlx() {
		return usbzlx;
	}
	public void setUsbzlx(String usbzlx) {
		this.usbzlx = usbzlx;
	}
	public String getGaib() {
		return gaib;
	}
	public void setGaib(String gaib) {
		this.gaib = gaib;
	}
	public String getNeic() {
		return neic;
	}
	public void setNeic(String neic) {
		this.neic = neic;
	}
	public String getBaozlx() {
		return baozlx;
	}
	public void setBaozlx(String baozlx) {
		this.baozlx = baozlx;
	}
	public String getBaozmc() {
		return baozmc;
	}
	public void setBaozmc(String baozmc) {
		this.baozmc = baozmc;
	}
	public String getChangd() {
		return changd;
	}
	public void setChangd(String changd) {
		this.changd = changd;
	}
	public String getKuand() {
		return kuand;
	}
	public void setKuand(String kuand) {
		this.kuand = kuand;
	}
	public String getGaod() {
		return gaod;
	}
	public void setGaod(String gaod) {
		this.gaod = gaod;
	}
	public String getBaozzl() {
		return baozzl;
	}
	public void setBaozzl(String baozzl) {
		this.baozzl = baozzl;
	}
	public String getLeib() {
		return leib;
	}
	public void setLeib(String leib) {
		this.leib = leib;
	}
	public String getCaiz() {
		return caiz;
	}
	public void setCaiz(String caiz) {
		this.caiz = caiz;
	}
	public String getShifhs() {
		return shifhs;
	}
	public void setShifhs(String shifhs) {
		this.shifhs = shifhs;
	}
	public String getZhedgd() {
		return zhedgd;
	}
	public void setZhedgd(String zhedgd) {
		this.zhedgd = zhedgd;
	}
	public String getDuidcs() {
		return duidcs;
	}
	public void setDuidcs(String duidcs) {
		this.duidcs = duidcs;
	}
	public String getBaiffx() {
		return baiffx;
	}
	public void setBaiffx(String baiffx) {
		this.baiffx = baiffx;
	}
	
	
	
}
