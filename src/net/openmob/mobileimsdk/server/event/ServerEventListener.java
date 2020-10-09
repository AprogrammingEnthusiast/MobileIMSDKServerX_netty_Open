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
 * ServerEventListener.java at 2020-4-14 17:24:14, code by Jack Jiang.
 */
package net.openmob.mobileimsdk.server.event;

import io.netty.channel.Channel;
import net.openmob.mobileimsdk.server.protocal.Protocal;

public interface ServerEventListener {
    /**
     * 用户身份验证回调方法定义.
     * 服务端的应用层可在本方法中实现用户登陆验证。
     * 注意：本回调在一种特殊情况下——即用户实际未退出登陆但再次发起来登陆包时，本回调是不会被调用的！
     * <p>
     * 根据MobileIMSDK的算法实现，本方法中用户验证通过（即方法返回值=0时）后 ，将立即调用回调方法
     * onUserLoginAction_CallBack(int, String, IoSession)。 否则会将验证结果（本方法返回值错误码通过客户端的
     * ChatBaseEvent.onLoginMessage(int dwUserId, int dwErrorCode) 方法进行回调）通知客户端）。
     *
     * @param userId
     * @param token
     * @param extra
     * @param session
     * @return
     */
    public int onVerifyUserCallBack(String userId, String token, String extra, Channel session);

    /**
     * 用户登录验证成功后的回调方法定义（可理解为上线通知回调）.
     * 服务端的应用层通常可在本方法中实现用户上线通知等。
     * 注意：本回调在一种特殊情况下——即用户实际未退出登陆但再次发起来登陆包时，回调也是一定会被调用。
     *
     * @param userId
     * @param extra
     * @param session
     */
    public void onUserLoginAction_CallBack(String userId, String extra, Channel session);

    /**
     * 用户退出登录回调方法定义（可理解为下线通知回调）。
     * 服务端的应用层通常可在本方法中实现用户下线通知等。
     *
     * @param userId
     * @param obj
     * @param session
     */
    public void onUserLogoutAction_CallBack(String userId, Object obj, Channel session);

    //	public boolean onTransBuffer_CallBack(String userId, String from_user_id
//			, String dataContent, String fingerPrint, int typeu, Channel session);
    public boolean onTransBuffer_C2S_CallBack(Protocal p, Channel session);

    //	public void onTransBuffer_C2C_CallBack(String userId, String from_user_id
//			, String dataContent, String fingerPrint, int typeu);
    public void onTransBuffer_C2C_CallBack(Protocal p);

    //	public boolean onTransBuffer_C2C_RealTimeSendFaild_CallBack(String userId
//			, String from_user_id, String dataContent
//			, String fingerPrint, int typeu);
    public boolean onTransBuffer_C2C_RealTimeSendFaild_CallBack(Protocal p);

}
