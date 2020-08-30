/*
 * Copyright (C) 2020  即时通讯网(52im.net) & Jack Jiang.
 * The MobileIMSDK_X_netty (MobileIMSDK v4.x Netty版) Project. 
 * All rights reserved.
 * 
 * > Github地址：https://github.com/JackJiang2011/MobileIMSDK
 * > 文档地址：  http://www.52im.net/forum-89-1.html
 * > 技术社区：  http://www.52im.net/
 * > 技术交流群：320837163 (http://www.52im.net/topic-qqgroup.html)
 * > 作者公众号：“即时通讯技术圈】”，欢迎关注！
 * > 联系作者：  http://www.52im.net/thread-2792-1-1.html
 *  
 * "即时通讯网(52im.net) - 即时通讯开发者社区!" 推荐开源工程。
 * 
 * ServerLauncher.java at 2020-4-14 17:24:14, code by Jack Jiang.
 */
package net.openmob.mobileimsdk.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.timeout.ReadTimeoutHandler;

import java.io.IOException;

import net.nettime.mobileimsdk.server.bridge.QoS4ReciveDaemonC2B;
import net.nettime.mobileimsdk.server.bridge.QoS4SendDaemonB2C;
import net.nettime.mobileimsdk.server.netty.MBUDPClientInboundHandler;
import net.nettime.mobileimsdk.server.netty.MBUDPServerChannel;
import net.openmob.mobileimsdk.server.event.MessageQoSEventListenerS2C;
import net.openmob.mobileimsdk.server.event.ServerEventListener;
import net.openmob.mobileimsdk.server.qos.QoS4ReciveDaemonC2S;
import net.openmob.mobileimsdk.server.qos.QoS4SendDaemonS2C;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MobileIMSDK的服务端入口主类。
 * 为了简化API的调用，理论上使用者使用本类公开的方法即可实现MobileIMSDK 的所有能力。
 *
 * 基于Netty框架，使用UDP协议。本类主要实现服务端的初始化、基本服务的启停以及一些 提供给开发者的公开API。
 *
 * 服务端默认不会自动开启，请调用 startup()以便启动MobileIMSDK的服务端, shutdown()关闭 服务端，直到再次调用 startup() 。
 *
 * 提示1： 请重写 initListeners()方法，以实现应用层的回调监听器以便实现自定义业务 逻辑。
 *
 * 提示2： 如果设置整个MobileIMSDK的算法敏感度类型，请调用 ServerToolKits.setSenseMode(SenseMode)， MobileIMSDK默认敏感度是 ServerToolKits.SenseMode#MODE_3S。
 *
 * 提示3： 如要开启与MobileIMSDK-Web版的消息互通，请设置 bridgeEnabled == true ，默认为false.
 */
public abstract class ServerLauncher 
{
	private static Logger logger = LoggerFactory.getLogger(ServerLauncher.class); 

	@Deprecated
	/** 于2017年12月11日v3.1版废除本字段，为了性能请使用log4j的日志输出级别来控制日志输出 **/
	public static boolean debug = true;
	/** 全局设置：AppKey **/
	public static String appKey = null;
    public static int PORT = 7901;
    /** UDP Session的空闲超时时长（单位：秒），默认10秒 **/
    public static int SESION_RECYCLER_EXPIRE = 10;
    /** 是否允许与MobileIMSDK Web版进行互通 **/
    public static boolean bridgeEnabled = false;
    
    private boolean running = false;
    /** MobileIMSDK框架的核心通信逻辑实现类（实现的是MobileIMSDK服务端的通信处理核心算法） **/
    protected ServerCoreHandler serverCoreHandler = null;
    
 	private final EventLoopGroup __bossGroup4Netty = new NioEventLoopGroup();
 	private final EventLoopGroup __workerGroup4Netty = new DefaultEventLoopGroup();
 	private Channel __serverChannel4Netty = null;

 	public ServerLauncher() throws IOException 
 	{
 		// default do nothing
 	}

 	public boolean isRunning()
 	{
 		return running;
 	}

 	public void startup() throws Exception
 	{	
 		if(!this.running)
 		{
 			serverCoreHandler = initServerCoreHandler();

 			initListeners();

 			ServerBootstrap bootstrap = initServerBootstrap4Netty();

 			QoS4ReciveDaemonC2S.getInstance().startup();
 			QoS4SendDaemonS2C.getInstance().startup(true).setServerLauncher(this);

 			if(ServerLauncher.bridgeEnabled){

 				QoS4ReciveDaemonC2B.getInstance().startup();
 				QoS4SendDaemonB2C.getInstance().startup(true).setServerLauncher(this);

 				serverCoreHandler.lazyStartupBridgeProcessor();

 				logger.info("[IMCORE-netty] 配置项：已开启与MobileIMSDK Web的互通.");
 			}
 			else{
 				logger.info("[IMCORE-netty] 配置项：未开启与MobileIMSDK Web的互通.");
 			}

 			ChannelFuture cf = bootstrap.bind("0.0.0.0", PORT).syncUninterruptibly();
 			__serverChannel4Netty = cf.channel();

 			this.running = true;
 			logger.info("[IMCORE-netty] 基于MobileIMSDK的UDP服务正在端口" + PORT+"上监听中...");

 			__serverChannel4Netty.closeFuture().await();
 		}
 		else
 		{
 			logger.warn("[IMCORE-netty] 基于MobileIMSDK的UDP服务正在运行中" +
 					"，本次startup()失败，请先调用shutdown()后再试！");
 		}
    }

    public void shutdown()
    {
    	if (__serverChannel4Netty != null) 
    		__serverChannel4Netty.close();

		__bossGroup4Netty.shutdownGracefully();
		__workerGroup4Netty.shutdownGracefully();
		
    	QoS4ReciveDaemonC2S.getInstance().stop();
    	QoS4SendDaemonS2C.getInstance().stop();
    	
    	if(ServerLauncher.bridgeEnabled){
    		QoS4ReciveDaemonC2B.getInstance().stop();
    		QoS4SendDaemonB2C.getInstance().stop();
    	}
    	
    	this.running = false;
    }
    
    protected ServerCoreHandler initServerCoreHandler()
    {
    	return new ServerCoreHandler();
    }
    
    protected abstract void initListeners();
    
    protected ServerBootstrap initServerBootstrap4Netty()
    {
    	return new ServerBootstrap()
    		.group(__bossGroup4Netty, __workerGroup4Netty)
    		.channel(MBUDPServerChannel.class)
    		.childHandler(initChildChannelHandler4Netty());
    }
    
	protected ChannelHandler initChildChannelHandler4Netty()
	{
		return new ChannelInitializer<Channel>() {
			@Override
			protected void initChannel(Channel channel) throws Exception {
				channel.pipeline()
					.addLast(new ReadTimeoutHandler(SESION_RECYCLER_EXPIRE))
					.addLast(new MBUDPClientInboundHandler(serverCoreHandler));
			}
		};
	}
    
    public ServerEventListener getServerEventListener()
	{
		return serverCoreHandler.getServerEventListener();
	}
	public void setServerEventListener(ServerEventListener serverEventListener)
	{
		this.serverCoreHandler.setServerEventListener(serverEventListener);
	}
	
	public MessageQoSEventListenerS2C getServerMessageQoSEventListener()
	{
		return serverCoreHandler.getServerMessageQoSEventListener();
	}
	public void setServerMessageQoSEventListener(MessageQoSEventListenerS2C serverMessageQoSEventListener)
	{
		this.serverCoreHandler.setServerMessageQoSEventListener(serverMessageQoSEventListener);
	}

	public ServerCoreHandler getServerCoreHandler()
	{
		return serverCoreHandler;
	}
}
