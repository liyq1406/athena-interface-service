/**
 * 
 */
package com.athena.component;

import java.util.Locale;

import com.toft.core2.control.UserNumberCtrl;
import com.toft.core3.context.SdcContextEvent;
import com.toft.core3.context.SdcContextListener;
import com.toft.core3.i18n.CachingResourceBundleFactory;

/**
 * <p>Title:</p>
 *
 * <p>Description:</p>
 *
 * <p>Copyright:Copyright (c) 2011.9</p>
 *
 * <p>Company:iSoftstone</p>
 *
 * @author Administrator
 * @version 1.0
 */
public class StartListener implements SdcContextListener{

	public void contextDestroyed(SdcContextEvent contextEvent) {
		
	}

	public void contextInitialized(SdcContextEvent contextEvent) {
		UserNumberCtrl.setMaxNumber(100000);
		System.out.println("****************************ATHENA 系统启动*********************");
		System.out.println("****************当前系统语言环境："+CachingResourceBundleFactory.getDefaultLocale().getDisplayName()+"*********************");
		//设置默认的LOCALE
		Locale.setDefault(Locale.SIMPLIFIED_CHINESE);
		//TODO 根据用户配置选择启用语言
		System.out.println("****************设置系统语言环境："+CachingResourceBundleFactory.getDefaultLocale().getDisplayName()+"*********************");
	}

}
