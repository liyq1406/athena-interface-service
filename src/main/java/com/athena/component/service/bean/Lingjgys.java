package com.athena.component.service.bean;

import java.math.BigDecimal;
/**
 * 
 * @author 贺志国
 * @E-mail zghe@isoftstone.com
 * @version v1.0
 * @date 2015-7-23
 */
public class Lingjgys {
	private String usercenter;  //用户中心
	private String lingjbh;//零件编号
	private String gongysbh; //供应商编号
	private String ucbzlx; //UC包装类型
	private BigDecimal ucrl;//UC容量
	private String uabzlx;//UA包装类型
	private BigDecimal uarl;//UA容量
	private BigDecimal uaucgs;//UA中UC个数
	private String gaib;//盖板
	private String neic;//内衬
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
	public String getUcbzlx() {
		return ucbzlx;
	}
	public void setUcbzlx(String ucbzlx) {
		this.ucbzlx = ucbzlx;
	}
	public BigDecimal getUcrl() {
		return ucrl;
	}
	public void setUcrl(BigDecimal ucrl) {
		this.ucrl = ucrl;
	}
	public String getUabzlx() {
		return uabzlx;
	}
	public void setUabzlx(String uabzlx) {
		this.uabzlx = uabzlx;
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
}
