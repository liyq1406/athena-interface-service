package com.athena.component.service;

import java.util.List;

import javax.jws.WebParam;
import javax.jws.WebService;


/**
 * 调度接口
 * @author chenlei
 * @vesion 1.0
 * @date 2012-4-12
 */
@WebService
public interface DispatchService {
	
	/**
	 * 调度接口任务  一次调度一个接口任务
	 */
	public int dispatchTask(@WebParam(name="taskName")String taskName);
	/**
	 * 调度接口任务  一次调度多个接口任务
	 */
	public void dispatchTasks(@WebParam(name="taskNames")List<String> taskNames);
}
