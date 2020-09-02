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
 * MBUDPServerChannelConfig.java at 2020-4-14 17:24:14, code by Jack Jiang.
 */
package net.nettime.mobileimsdk.server.netty;

import java.net.SocketException;
import java.nio.channels.DatagramChannel;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelException;
import io.netty.channel.DefaultChannelConfig;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.MessageSizeEstimator;
import io.netty.channel.RecvByteBufAllocator;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.socket.ServerSocketChannelConfig;

/**
 * 仿照TCP的“形”实现了一个UDP的服务端Channel配置类。
 *
 * 因为Netty4中，对于ServerBootstrap来说为了灵活应对各种场景， 有很多参数可供设置，所以从设计模式的角度讲，
 * 就把这部分配置 独立成了遵从Builder设计模式的封装类，让ServerBootstrap的 API显的更优雅。
 */
public class MBUDPServerChannelConfig extends DefaultChannelConfig implements ServerSocketChannelConfig 
{
	/** UDP NIO channel通道实例 **/
	private final DatagramChannel datagramChannel;

	public MBUDPServerChannelConfig(Channel channel, DatagramChannel datagramChannel) 
	{
		super(channel);
		this.datagramChannel = datagramChannel;
		
		setRecvByteBufAllocator(new FixedRecvByteBufAllocator(2048));
	}

	/**
	 * backlog参数主要用于TCP场景下，是内核为此套接口 排队的最大连接个数，UDP时给1就行了，因为没意义。
	 * @return
	 */
	@Override
	public int getBacklog()
	{
		return 1;
	}

	/**
	 * backlog参数对于UDP来说无意义，它主要用于TCP场景下，是内核为此套接口 排队的最大连接个数，因而本方法只是个空方法。
	 * @param backlog
	 * @return
	 */
	@Override
	public ServerSocketChannelConfig setBacklog(int backlog)
	{
		return this;
	}

	/**
	 * connectTimeoutMillis对于UDP来说无意义，因而本方法只是个空方法。
	 * @param timeout
	 * @return
	 */
	@Override
	public ServerSocketChannelConfig setConnectTimeoutMillis(int timeout) 
	{
		return this;
	}

	/**
	 * 本方法对于UDP来说无意义，因而本方法只是个空方法。
	 * @param arg0
	 * @param arg1
	 * @param arg2
	 * @return
	 */
	@Override
	public ServerSocketChannelConfig setPerformancePreferences(int arg0, int arg1, int arg2) 
	{
		return this;
	}

	@Override
	public ServerSocketChannelConfig setAllocator(ByteBufAllocator alloc) 
	{
		super.setAllocator(alloc);
		return this;
	}

	@Override
	public ServerSocketChannelConfig setRecvByteBufAllocator(RecvByteBufAllocator alloc) 
	{
		super.setRecvByteBufAllocator(alloc);
		return this;
	}

	@Override
	public ServerSocketChannelConfig setAutoRead(boolean autoread) 
	{
		super.setAutoRead(true);
		return this;
	}

	@Override
	@Deprecated
	public ServerSocketChannelConfig setMaxMessagesPerRead(int n) 
	{
		super.setMaxMessagesPerRead(n);
		return this;
	}

	@Override
	public ServerSocketChannelConfig setMessageSizeEstimator(MessageSizeEstimator est)
	{
		super.setMessageSizeEstimator(est);
		return this;
	}

	@Override
	public ServerSocketChannelConfig setWriteSpinCount(int spincount)
	{
		super.setWriteSpinCount(spincount);
		return this;
	}

	public ServerSocketChannelConfig setWriteBufferHighWaterMark(int writeBufferHighWaterMark)
	{
		return (ServerSocketChannelConfig) super.setWriteBufferHighWaterMark(writeBufferHighWaterMark);
	}

	public ServerSocketChannelConfig setWriteBufferLowWaterMark(int writeBufferLowWaterMark) 
	{
		return (ServerSocketChannelConfig) super.setWriteBufferLowWaterMark(writeBufferLowWaterMark);
	}

	public ServerSocketChannelConfig setWriteBufferWaterMark(WriteBufferWaterMark writeBufferWaterMark) 
	{
		return (ServerSocketChannelConfig) super.setWriteBufferWaterMark(writeBufferWaterMark);
	}

	@Override
	public int getReceiveBufferSize() 
	{
		try
		{
			return datagramChannel.socket().getReceiveBufferSize();
		} 
		catch (SocketException ex)
		{
			throw new ChannelException(ex);
		}
	}

	@Override
	public ServerSocketChannelConfig setReceiveBufferSize(int size)
	{
		try
		{
			datagramChannel.socket().setReceiveBufferSize(size);
		} 
		catch (SocketException ex) 
		{
			throw new ChannelException(ex);
		}
		return this;
	}

	@Override
	public boolean isReuseAddress()
	{
		try
		{
			return datagramChannel.socket().getReuseAddress();
		}
		catch (SocketException ex) 
		{
			throw new ChannelException(ex);
		}
	}

	@Override
	public ServerSocketChannelConfig setReuseAddress(boolean reuseaddr)
	{
		try 
		{
			datagramChannel.socket().setReuseAddress(true);
		} 
		catch (SocketException ex) 
		{
			throw new ChannelException(ex);
		}
		return this;
	}
}
