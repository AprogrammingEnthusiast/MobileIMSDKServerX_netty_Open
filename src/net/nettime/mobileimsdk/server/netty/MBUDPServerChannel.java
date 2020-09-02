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
 * MBUDPServerChannel.java at 2020-4-14 17:24:14, code by Jack Jiang.
 */
package net.nettime.mobileimsdk.server.netty;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardProtocolFamily;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.spi.SelectorProvider;
import java.util.LinkedHashMap;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.RecvByteBufAllocator;
import io.netty.channel.nio.AbstractNioMessageChannel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.ServerSocketChannelConfig;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.internal.PlatformDependent;

/**
 * 仿照TCP协议的NioServerSocketChannel实现的专用于UDP的服务端Channel实现类。
 * {@link NioServerSocketChannel} 类似netty原生提供的socket服务端类库
 */
public class MBUDPServerChannel extends AbstractNioMessageChannel implements ServerSocketChannel 
{
	private final ChannelMetadata METADATA = new ChannelMetadata(true);
	
	private final MBUDPServerChannelConfig config;
	
	protected final LinkedHashMap<InetSocketAddress, MBUDPChannel> channels = new LinkedHashMap<InetSocketAddress, MBUDPChannel>();

	public MBUDPServerChannel() throws IOException
	{
		//SelectorProvider.provider()    .openDatagramChannel		   		   StandardProtocolFamily.INET
		//返回系统范围内的默认选择器提供程序 ｜ 打开并创建一个操作系统支持的UDP channel ｜ 互联网协议版本4（IPv4）
		this(SelectorProvider.provider().openDatagramChannel(StandardProtocolFamily.INET));
	}

	/**
	 * 传入处理UDP的channel
	 * @param datagramChannel UDP channel
	 */
	protected MBUDPServerChannel(DatagramChannel datagramChannel)
	{
		//构建底层NIO channel通道实例 @{see io.netty.channel.nio.AbstractNioChannel.AbstractNioChannel}
		super(null, datagramChannel, SelectionKey.OP_READ);
		this.config = new MBUDPServerChannelConfig(this, datagramChannel);
	}
	
	@Override
	public InetSocketAddress localAddress()
	{
		return (InetSocketAddress) super.localAddress();
	}
	
	@Override
	protected SocketAddress localAddress0() 
	{
		return this.javaChannel().socket().getLocalSocketAddress();
	}
	
	@Override
	public InetSocketAddress remoteAddress()
	{
		return null;
	}

	@Override
	protected SocketAddress remoteAddress0()
	{
		return null;
	}
	
	@Override
	public ChannelMetadata metadata()
	{
		return METADATA;
	}
	
	@Override
	public ServerSocketChannelConfig config()
	{
		return config;
	}
	
	/**
	 * 判断监听是否已启动。
	 * 
	 * @return true表示已已启动监听，否则未启动
	 * @see DatagramChannel#isOpen()
	 * @see DatagramSocket#isBound()
	 */
	@Override
	public boolean isActive() 
	{
		return this.javaChannel().isOpen() && this.javaChannel().socket().isBound();
	}
	
	@Override
	protected DatagramChannel javaChannel() 
	{
		return (DatagramChannel) super.javaChannel();
	}

	@Override
	protected void doBind(SocketAddress localAddress) throws Exception 
	{
		javaChannel().socket().bind(localAddress);
	}

	@Override
	protected void doClose() throws Exception
	{
		for (MBUDPChannel channel : channels.values())
			channel.close();
		
		javaChannel().close();
	}

	/**
	 * 将一个客户端的Channel实例从服务端管理的列表中移除。
	 *
	 * @param channel
	 */
	public void removeChannel(final Channel channel) 
	{
		eventLoop().submit(new Runnable() {
			@Override
			public void run() {
				InetSocketAddress remote = (InetSocketAddress) channel.remoteAddress();
				if (channels.get(remote) == channel) 
				{
					channels.remove(remote);
				}
			}
		});
	}

	@Override
	protected int doReadMessages(List<Object> list) throws Exception
	{
		//获取java原生nio提供UDP的channel
		DatagramChannel javaChannel = javaChannel();
		//unsafe     在netty中一个很核心的组件，封装了java底层的socket操作，作为连接netty和java 底层nio的重要桥梁。
		//获取一个自适应的缓冲区分配器
		RecvByteBufAllocator.Handle allocatorHandle = unsafe().recvBufAllocHandle();
		//分配一个缓冲
		ByteBuf buffer = allocatorHandle.allocate(config.getAllocator());
		//将通道中的数据读取到缓冲中
		allocatorHandle.attemptedBytesRead(buffer.writableBytes());

		boolean freeBuffer = true;
		try 
		{
			// 将ByteBuf中可写的ByteBuffer取出
			ByteBuffer nioBuffer = buffer.internalNioBuffer(buffer.writerIndex(), buffer.writableBytes());
			//获取链接信息之前ByteBuffer写入位置
			// ======｜=============|
			//       ^			    ^
			//     position     capacity
			int nioPos = nioBuffer.position();

			// 获取客户端链接ip和port
			// ======｜====IP/port===|===========|
			//       ^		      	 ^	         ^
			//     nioPos	     position     capacity
			InetSocketAddress inetSocketAddress = (InetSocketAddress) javaChannel.receive(nioBuffer);
			if (inetSocketAddress == null) 
				return 0;

			//设置上次读取操作已读取的字节。可用于增加以读取的字节数
			allocatorHandle.lastBytesRead(nioBuffer.position() - nioPos);
			buffer.writerIndex(buffer.writerIndex() + allocatorHandle.lastBytesRead());
			
			// 分配新channel或使用现有channel并将消息推送到该channel
			MBUDPChannel udpchannel = channels.get(inetSocketAddress);
			//如果address对应channel为空，则创建新channel
			if ((udpchannel == null) || !udpchannel.isOpen()) 
			{
				udpchannel = new MBUDPChannel(this, inetSocketAddress);
				channels.put(inetSocketAddress, udpchannel);
				list.add(udpchannel);
				
				udpchannel.addBuffer(buffer);
				freeBuffer = false;
				
				return 1;
			} 
			else
			{
				udpchannel.addBuffer(buffer);
				freeBuffer = false;
				
				if (udpchannel.isRegistered()) 
					udpchannel.read();
				
				return 0;
			}
		} 
		catch (Throwable t) 
		{
			PlatformDependent.throwException(t);
			return -1;
		} 
		finally
		{
			if (freeBuffer)
				// 如果属于无用buffer，需要即使回收
				buffer.release();
		}
	}

	@Override
	protected boolean doWriteMessage(Object msg, ChannelOutboundBuffer buffer) throws Exception
	{
		DatagramPacket dpacket = (DatagramPacket) msg;
		InetSocketAddress recipient = dpacket.recipient();
		ByteBuf byteBuf = dpacket.content();
		int readableBytes = byteBuf.readableBytes();
		if (readableBytes == 0) 
			return true;
		
		ByteBuffer internalNioBuffer = byteBuf.internalNioBuffer(
				byteBuf.readerIndex(), readableBytes);
		
		return javaChannel().send(internalNioBuffer, recipient) > 0;
	}

	@Override
	protected boolean doConnect(SocketAddress addr1, SocketAddress addr2) throws Exception 
	{
		throw new UnsupportedOperationException();
	}

	@Override
	protected void doFinishConnect() throws Exception
	{
		throw new UnsupportedOperationException();
	}

	@Override
	protected void doDisconnect() throws Exception 
	{
		throw new UnsupportedOperationException();
	}
}
