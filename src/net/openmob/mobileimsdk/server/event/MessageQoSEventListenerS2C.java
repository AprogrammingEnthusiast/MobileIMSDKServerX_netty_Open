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
 * MessageQoSEventListenerS2C.java at 2020-4-14 17:24:15, code by Jack Jiang.
 */
package net.openmob.mobileimsdk.server.event;

import java.util.ArrayList;

import net.openmob.mobileimsdk.server.protocal.Protocal;

public interface MessageQoSEventListenerS2C
{
	void messagesLost(ArrayList<Protocal> lostMessages);
	void messagesBeReceived(String theFingerPrint);
}
