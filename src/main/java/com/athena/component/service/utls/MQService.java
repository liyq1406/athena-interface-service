package com.athena.component.service.utls;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.log4j.Logger;

import com.ibm.mq.MQC;
import com.ibm.mq.MQEnvironment;
import com.ibm.mq.MQException;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQPutMessageOptions;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;


/**
 * 
 * @version v1.0
 * @date 2016-1-15
 */
public class MQService {
	protected static Logger logger = Logger.getLogger(MQService.class);	//定义日志方法
	public void sendMSG(String uri,String serviceName){
		MQConfig config = MQConfig.getNewDbConfigFromKey();
		MQQueueManager qm = null;
		try {
			qm = createMQmanage(config.getMQ_HOST_NAME(), config.getMQ_PROT(), config.getMQ_CHANNEL(), config.getMQ_MANAGER(), config.getMQ_CCSID());
			File file = new File(uri);
			sendMessageFile(file, qm, config.getMQ_MANAGER(), config.getMQ_QUEUE_NAME());
		} catch (Exception e) {
			logger.error(e.getMessage());
		} finally {
			try {
				qm.close();
			} catch (MQException e) {
				logger.error(e.getMessage());
			}
		}
	}

	/**
	 * 往MQ发送消息
	 * 
	 * @param message
	 * @return
	 * @throws MQException
	 * @throws IOException
	 */
	public int sendMessage(String message, MQQueueManager qMgr, String qmName,
			String qName) throws MQException, IOException {
		int result = 0;
		try {
			// 设置将要连接的队列属性
			// Note. The MQC interface defines all the constants used by the
			// WebSphere MQ Java programming interface
			// (except for completion code constants and error code constants).
			// MQOO_INPUT_AS_Q_DEF:Open the queue to get messages using the
			// queue-defined default.
			// MQOO_OUTPUT:Open the queue to put messages.
			/* 目标为远程队列，所有这里不可以用MQOO_INPUT_AS_Q_DEF属性 */
			// int openOptions = MQC.MQOO_INPUT_AS_Q_DEF | MQC.MQOO_OUTPUT;
			/* 以下选项可适合远程队列与本地队列 */
			int openOptions = MQC.MQOO_OUTPUT | MQC.MQOO_FAIL_IF_QUIESCING;
			// 连接队列
			// MQQueue provides inquire, set, put and get operations for
			// WebSphere MQ queues.
			// The inquire and set capabilities are inherited from
			// MQManagedObject.
			/* 关闭了就重新打开 */
			if (qMgr == null || !qMgr.isConnected()) {
				MQEnvironment.properties.put(MQC.TRANSPORT_PROPERTY,
						MQC.TRANSPORT_MQSERIES);
				qMgr = new MQQueueManager(qmName);
			}
			MQQueue queue = qMgr.accessQueue(qName, openOptions);
			// 定义一个简单的消息
			MQMessage putMessage = new MQMessage();
			// 将数据放入消息缓冲区
			putMessage.writeUTF(message);
			// 设置写入消息的属性（默认属性）
			MQPutMessageOptions pmo = new MQPutMessageOptions();
			// 将消息写入队列
			queue.put(putMessage, pmo);
			queue.close();
		} catch (MQException ex) {
			logger.error("A WebSphere MQ error occurred : Completion code "
							+ ex.completionCode + " Reason code "
							+ ex.reasonCode);
			// ex.printStackTrace();
			throw ex;
		} catch (IOException ex) {
			logger.error("An error occurred whilst writing to the message buffer: "+ ex);
			throw ex;
		} finally {
			qMgr.disconnect();
		}
		return result;
	}

	/**
	 * 往MQ发送消息
	 * 
	 * @param message
	 *            参数 待发送的文件,MQ服务类,队列管理器,队列
	 * @return
	 * @throws MQException
	 * @throws IOException
	 */
	public int sendMessageFile(File file, MQQueueManager qMgr, String qmName,
			String qName) throws MQException, IOException {
		int result = 0;
		try {
			// 设置将要连接的队列属性
			// Note. The MQC interface defines all the constants used by the
			// WebSphere MQ Java programming interface
			// (except for completion code constants and error code constants).
			// MQOO_INPUT_AS_Q_DEF:Open the queue to get messages using the
			// queue-defined default.
			// MQOO_OUTPUT:Open the queue to put messages.
			/* 目标为远程队列，所有这里不可以用MQOO_INPUT_AS_Q_DEF属性 */
			// int openOptions = MQC.MQOO_INPUT_AS_Q_DEF | MQC.MQOO_OUTPUT;
			/* 以下选项可适合远程队列与本地队列 */
			int openOptions = MQC.MQOO_OUTPUT | MQC.MQOO_FAIL_IF_QUIESCING;
			// 连接队列
			// MQQueue provides inquire, set, put and get operations for
			// WebSphere MQ queues.
			// The inquire and set capabilities are inherited from
			// MQManagedObject.
			/* 关闭了就重新打开 */
			if (qMgr == null || !qMgr.isConnected()) {
				MQEnvironment.properties.put(MQC.TRANSPORT_PROPERTY,
						MQC.TRANSPORT_MQSERIES);
				qMgr = new MQQueueManager(qmName);
			}
			MQQueue queue = qMgr.accessQueue(qName, openOptions);
			// 定义一个简单的消息
			MQMessage putMessage = new MQMessage();
			putMessage.characterSet = 1208;
			putMessage.encoding = 273;
			putMessage.format = MQC.MQFMT_NONE;

			// 将数据放入消息缓冲区
			putMessage.write(getFileByteContent(file.getPath()));

			int lenTotal = putMessage.getMessageLength();
			logger.info("lenTotal of msg = " + lenTotal);

			// 存放文件名称
			// putMessage.replyToQueueManagerName=file.getName();
			// System.out.println("replyToQueueManagerName = " +
			// putMessage.replyToQueueManagerName);

			// 设置写入消息的属性（默认属性）
			MQPutMessageOptions pmo = new MQPutMessageOptions();
			// 将消息写入队列
			queue.put(putMessage, pmo);
			logger.info("send file is success!");
			/* release resource */
			queue.close();// 关闭队列
			qMgr.disconnect();// 断开消息队列管理器
			logger.info("recieve finish.");
		} catch (MQException ex) {
			logger.error("A WebSphere MQ error occurred : Completion code "
							+ ex.completionCode + " Reason code "
							+ ex.reasonCode);
			throw ex;
		} catch (IOException ex) {
			logger.error("An error occurred whilst writing to the message buffer: "+ ex);
			throw ex;
		} finally {
			qMgr.disconnect();
		}
		return result;
	}
	
	/**
	 * 往MQ发送消息
	 * 
	 * @param message
	 *            参数 待发送的文件,MQ服务类,队列管理器,队列
	 * @return
	 * @throws MQException
	 * @throws IOException
	 */
	public int sendMessage(File file, MQQueueManager qMgr, String qmName,
			String qName) throws MQException, IOException {
		int result = 0;
		try {
			// 设置将要连接的队列属性
			// Note. The MQC interface defines all the constants used by the
			// WebSphere MQ Java programming interface
			// (except for completion code constants and error code constants).
			// MQOO_INPUT_AS_Q_DEF:Open the queue to get messages using the
			// queue-defined default.
			// MQOO_OUTPUT:Open the queue to put messages.
			/* 目标为远程队列，所有这里不可以用MQOO_INPUT_AS_Q_DEF属性 */
			// int openOptions = MQC.MQOO_INPUT_AS_Q_DEF | MQC.MQOO_OUTPUT;
			/* 以下选项可适合远程队列与本地队列 */
			int openOptions = MQC.MQOO_OUTPUT | MQC.MQOO_FAIL_IF_QUIESCING;
			// 连接队列
			// MQQueue provides inquire, set, put and get operations for
			// WebSphere MQ queues.
			// The inquire and set capabilities are inherited from
			// MQManagedObject.
			/* 关闭了就重新打开 */
			if (qMgr == null || !qMgr.isConnected()) {
				MQEnvironment.properties.put(MQC.TRANSPORT_PROPERTY,
						MQC.TRANSPORT_MQSERIES);
				qMgr = new MQQueueManager(qmName);
			}
			MQQueue queue = qMgr.accessQueue(qName, openOptions);
			// 定义一个简单的消息
			MQMessage putMessage = new MQMessage();
			putMessage.characterSet = 1208;
			putMessage.encoding = 273;
			putMessage.format = MQC.MQFMT_NONE;

			// 将数据放入消息缓冲区
			putMessage.write(getFileByteContent(file.getPath()));

			int lenTotal = putMessage.getMessageLength();
			logger.info("lenTotal of msg = " + lenTotal);

			// 存放文件名称
			// putMessage.replyToQueueManagerName=file.getName();
			// System.out.println("replyToQueueManagerName = " +
			// putMessage.replyToQueueManagerName);

			// 设置写入消息的属性（默认属性）
			MQPutMessageOptions pmo = new MQPutMessageOptions();
			// 将消息写入队列
			queue.put(putMessage, pmo);
			logger.info("send file is success!");
			/* release resource */
			queue.close();// 关闭队列
			qMgr.disconnect();// 断开消息队列管理器
			logger.info("recieve finish.");
		} catch (MQException ex) {
			logger.error("A WebSphere MQ error occurred : Completion code "
							+ ex.completionCode + " Reason code "
							+ ex.reasonCode);
			// ex.printStackTrace();
			throw ex;
		} catch (IOException ex) {
			logger.error("An error occurred whilst writing to the message buffer: "
							+ ex);
			throw ex;
		} finally {
			qMgr.disconnect();
		}
		return result;
	}

	/**
	 * 从队列中去获取消息，如果队列中没有消息，就会发生异常，不过没有关系，有TRY...CATCH，如果是第三方程序调用方法，如果无返回则说明无消息
	 * 第三方可以将该方法放于一个无限循环的while(true){...}之中，不需要设置等待，因为在该方法内部在没有消息的时候会自动等待。
	 * 
	 * @return
	 * @throws MQException
	 * @throws IOException
	 */
	public static String getMessage(MQQueueManager qMgr, String qmName,
			String qName) throws MQException, IOException {
		String message = null;
		try {
			// 设置将要连接的队列属性
			// Note. The MQC interface defines all the constants used by the
			// WebSphere MQ Java programming interface
			// (except for completion code constants and error code constants).
			// MQOO_INPUT_AS_Q_DEF:Open the queue to get messages using the
			// queue-defined default.
			// MQOO_OUTPUT:Open the queue to put messages.
			int openOptions = MQC.MQOO_INPUT_AS_Q_DEF | MQC.MQOO_OUTPUT;
			MQMessage retrieve = new MQMessage();
			// 设置取出消息的属性（默认属性）
			// Set the put message options.（设置放置消息选项）
			MQGetMessageOptions gmo = new MQGetMessageOptions();
			gmo.options = gmo.options + MQC.MQGMO_SYNCPOINT;// Get messages
															// under sync point
															// control（在同步点控制下获取消息）
			gmo.options = gmo.options + MQC.MQGMO_WAIT; // Wait if no messages
														// on the
														// Queue（如果在队列上没有消息则等待）
			gmo.options = gmo.options + MQC.MQGMO_FAIL_IF_QUIESCING;// Fail if
																	// Qeue
																	// Manager
																	// Quiescing（如果队列管理器停顿则失败）
			gmo.waitInterval = 1000; // Sets the time limit for the
										// wait.（设置等待的毫秒时间限制）
			/* 关闭了就重新打开 */
			if (qMgr == null || !qMgr.isConnected()) {
				qMgr = new MQQueueManager(qmName);
			}
			MQQueue queue = qMgr.accessQueue(qName, openOptions);
			// 从队列中取出消息
			queue.get(retrieve, gmo);
			message = retrieve.readUTF();
			logger.info("The message is: " + message);
			queue.close();
		} catch (MQException ex) {
			logger.error("A WebSphere MQ error occurred : Completion code "
							+ ex.completionCode + " Reason code "
							+ ex.reasonCode);
			throw ex;
		} catch (IOException ex) {
			logger.error("An error occurred whilst writing to the message buffer: "
							+ ex);
			throw ex;
		} finally {
			try {
				qMgr.disconnect();
			} catch (MQException e) {
				throw e;
			}
		}
		return message;
	}

	/**
	 * 
	 * 从队列中去获取消息，如果队列中没有消息，就会发生异常，不过没有关系，有TRY...CATCH，如果是第三方程序调用方法，如果无返回则说明无消息
	 * 第三方可以将该方法放于一个无限循环的while(true){...}之中，不需要设置等待，因为在该方法内部在没有消息的时候会自动等待。
	 * 
	 * @return 参数 qMgr服务对象,队列管理器,队列,接收文件路径
	 * @throws MQException
	 * @throws IOException
	 * 
	 **/
	public void receiveFile(MQQueueManager qMgr, String qmName, String qName,
			String rootDir) throws MQException, IOException {
		logger.info("read .......");
		try {

			/*
			 * 设置打开选项以便打开用于输出的队列，如果队列管理器停止，我们也 已设置了选项去应对不成功情况
			 */
			int openOptions = MQC.MQOO_INPUT_SHARED
					| MQC.MQOO_FAIL_IF_QUIESCING | MQC.MQOO_INQUIRE;

			if (qMgr == null || !qMgr.isConnected()) {
				qMgr = new MQQueueManager(qmName);
			}
			MQQueue q = qMgr.accessQueue(qName, openOptions, null, null, null);

			int depth = q.getCurrentDepth();

			if (depth > 0) {
				openOptions = MQC.MQOO_INPUT_SHARED
						| MQC.MQOO_FAIL_IF_QUIESCING;

				for (int i = 0; i < depth; i++) {
					/* 打开队列 */
					MQQueue queue = qMgr.accessQueue(qName, openOptions);
					logger.info(" 队列连接成功 ");
					/* 设置放置消息选项 */
					MQGetMessageOptions gmo = new MQGetMessageOptions();

					/* 在同步点控制下获取消息 */
					gmo.options = gmo.options + MQC.MQGMO_SYNCPOINT;

					/* 如果在队列上没有消息则等待 */
					gmo.options = gmo.options + MQC.MQGMO_WAIT;

					/* 如果队列管理器停顿则失败 */
					gmo.options = gmo.options + MQC.MQGMO_FAIL_IF_QUIESCING;

					/* 设置等待的时间限制 */
					gmo.waitInterval = 30000;
					/* create the message buffer store */
					MQMessage inMessage = new MQMessage();
					inMessage.characterSet = 1208;
					inMessage.format = MQC.MQFMT_NONE;
					/* get the message */
					queue.get(inMessage, gmo);

					// 获取文件名称
					// String filename = inMessage.replyToQueueManagerName;
					// System.out.println("filename = " + filename);
					SimpleDateFormat sf = new SimpleDateFormat(
							"yyyyMMddHHmmssSSS");
					String filename = sf.format(new Date());
					int len = inMessage.getDataLength();
					logger.info("filelength DataLength = " + len);
					logger.info("filelength MessageLength = "
							+ inMessage.getMessageLength());
					byte[] body = new byte[len];
					inMessage.readFully(body);
					/* save file */
					String fullname = writeFile(filename, body, rootDir);
					logger.info(fullname);
					// 清楚该条MSG
					// if (inMessage.readBoolean()) {
					// inMessage.clearMessage();
					// }
					/* process data */
					// accessDatabase(fullname);
				}
				// System.out.println(messageObject.get("FileName"));
				/* 提交事务 , 相当于确认消息已经接收，服务器会删除该消息 */
				qMgr.commit();

				logger.info("read finish.");
			} else {// 没有可以接收的文件
				logger.info("MQ服务器没有可以接受的文件!");
			}
		} catch (MQException e) {
			// e.printStackTrace();
			throw e;
		} catch (IOException e) {
			// e.printStackTrace();
			throw e;
		} finally {
			if (qMgr != null) {
				try {
					qMgr.disconnect();
				} catch (MQException e) {
					// e.printStackTrace();
					throw e;
				}
			}
		}
	}

	public static byte[] getFileByteContent(String fileName) throws IOException {
		FileInputStream fis = null;
		FileChannel fcin = null;
		try {
			File file = new File(fileName);
			fis = new FileInputStream(fileName);
			fcin = fis.getChannel();
			ByteBuffer buffer = ByteBuffer.allocate((int) file.length());
			fcin.read(buffer);
			return buffer.array();
		} finally {
			if (fcin != null) {
				fcin.close();
			}
			if (fis != null) {
				fis.close();
			}
		}
	}

	private String writeFile(String filename, byte[] data, String rootDir)
			throws IOException {
		FileOutputStream output = null;
		try {

			rootDir = rootDir.replace("\\", "/");// +"/g95backup";
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
			String date = sdf.format(Calendar.getInstance().getTime());
			// String dir = rootDir + "/" + date;
			String dir = rootDir + "/";
			File backupDir = new File(dir);
			if (!backupDir.exists()) {
				boolean isMkdirs = backupDir.mkdirs();
				if (!isMkdirs) {
					throw new RuntimeException("not cant mkdirs ");
				}
			}

			File file = new File(backupDir, filename);
			output = new FileOutputStream(file);
			output.write(data);
			output.close();

			return file.getAbsolutePath();
		} finally {
			if (output != null) {
				output.close();
			}
		}
	}

	public MQQueueManager createMQmanage(String hostname, String port,
			String channel, String qmname, String ccsid)
			throws MQException {

		// 设置环境:
		// MQEnvironment中包含控制MQQueueManager对象中的环境的构成的静态变量，MQEnvironment的值的设定会在MQQueueManager的构造函数加载的时候起作用，
		// 因此必须在建立MQQueueManager对象之前设定MQEnvironment中的值.
		MQEnvironment.hostname = hostname; // MQ服务器的IP地址 10.24.1.180
		MQEnvironment.channel = channel; // 服务器连接的通道 S_FENGLB
		MQEnvironment.CCSID = Integer.parseInt(ccsid); // 服务器MQ服务使用的编码1381代表GBK、1208代表UTF(Coded
														// Character Set
														// Identifier:CCSID)
		MQEnvironment.port = Integer.parseInt(port); // MQ端口
		// qmName = qmname; // MQ的队列管理器名称
		// qName = qname; // MQ远程队列的名称 testQ
		try {
			// 定义并初始化队列管理器对象并连接
			// MQQueueManager可以被多线程共享，但是从MQ获取信息的时候是同步的，任何时候只有一个线程可以和MQ通信。
			MQEnvironment.properties.put(MQC.TRANSPORT_PROPERTY,MQC.TRANSPORT_MQSERIES);
			// MQEnvironment.properties.put(MQC.TRANSPORT_PROPERTY,
			// MQC.TRANSPORT_MQSERIES_BINDINGS);
			return new MQQueueManager(qmname);

		} catch (MQException e) {
			logger.error("初使化MQ出错");
			throw e;
		}
	}
}