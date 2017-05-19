package com.athena.component.service.bean;
/**
 * 接口暴露的执行信息 order对象
 * @author chenlei
 * @vesion 1.0
 * @date 2012-5-25
 */
public class OrderConfig {
	private String id;
	private String name;
	private String flag; //是否继续执行下面的order true 为继续执行，false：中止不执行
	private String istxt; //是否为解析txt处理  true:是解析txt;false:向外分发
	private String isDb;  //针对2530特殊接口而定义的变量，是否走DB到txt接口 hzg 2013-3-15
	private String sort; // 针对sppv链的调用而定义的变更，是否抛异常给autosys，由此配置控制 hzg 2014.9.13 
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getFlag() {
		return flag;
	}
	public void setFlag(String flag) {
		this.flag = flag;
	}
	public String getIstxt() {
		return istxt;
	}
	public void setIstxt(String istxt) {
		this.istxt = istxt;
	}
	
	public String getIsDb() {
		return isDb;
	}
	public void setIsDb(String isDb) {
		this.isDb = isDb;
	}
	public String getSort() {
		return sort;
	}
	public void setSort(String sort) {
		this.sort = sort;
	}
	
	
}
