package com.athena.component.service.bean;

import java.util.ArrayList;
import java.util.List;

/**
 * 接口暴露的执行信息对象
 * @author chenlei
 * @vesion 1.0
 * @date 2012-5-25
 */
public class InterfaceConfig {
	private String id; //主键
	private String name; //名称
	private String type; //类型  in/out
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
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public List<OrderConfig> getOrders() {
		return orders;
	}
	public void setOrders(List<OrderConfig> orders) {
		this.orders = orders;
	}
	private List<OrderConfig> orders = new ArrayList<OrderConfig>();
}
