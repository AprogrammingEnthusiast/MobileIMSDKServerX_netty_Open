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
 * MQProvider.java at 2020-4-14 17:24:15, code by Jack Jiang.
 */
package net.nettime.mobileimsdk.server.bridge;

import java.io.IOException;
import java.util.Observer;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoveryListener;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * 一个可重用的RabbitMQ服务提供者类，可供处理一个生产者队列和一个消费者队列。
 * 本类的代码经过高度提炼和内聚，不限于MobileIMSDK使用，可用于任意合适场景。
 * <p>
 * 【本类中实现的功能有】：
 * 1）一个消息生产者（子类可决定是否使用）；
 * 2）一个消息消费者（子类可决定是否使用）；
 * 3）与MQ服务器的断线重连和恢复能力；
 * 4）发送出错暂存到缓存数组（内存中），并在下次重连正常时自动重发；
 * 5）遵照官方的最佳实践：复用同的是一个连接（connection）、各自两个channel（一个用于生产者、一个用于消费者）；
 * 6）消费者手动ACK能力：业务层处理不成功可重新放回队列。
 */
public class MQProvider {
    private static Logger logger = LoggerFactory.getLogger(MQProvider.class);

    /**
     * 发出消息的字符编码格式。
     */
    public final static String DEFAULT_ENCODE_CHARSET = "UTF-8";
    /**
     * 收到消息的字符解码格式。
     */
    public final static String DEFAULT_DECODE_CHARSET = "UTF-8";

    protected ConnectionFactory _factory = null;
    protected Connection _connection = null;
    protected Channel _pubChannel = null;

    /**
     * 【说明】：RabittMQ的Java客户端的automaticRecovery只在连接成功后才会启动，像
     * 这种首次连接时服务器根本就没开或者本地网络故障等，首次无法成功建立Connection的 ，
     * connction返回直接是null，当然就不存在automaticRecovery能力了，所以需要自已 来尝试重新start，一定要注意思路哦，别理解乱了。
     */
    protected final Timer timerForStartAgain = new Timer();
    /**
     * 此标识仅用于防止首次连接失败重试时因TimeTask的异步执行而发生重复执行的可能，仅此而已
     */
    protected boolean startRunning = false;

    /**
     * 本定时的作用是当worker启动或运行过程中出错时，可以自动进行恢复，而不至于丧失功能
     */
    protected final Timer timerForRetryWorker = new Timer();
    /**
     * 此标识仅用于防止worker失败重试时因TimeTask的异步执行而发生重复执行的可能，仅此而已
     */
    protected boolean retryWorkerRunning = false;

    /**
     * 本地生产者用的暂存消息队列：因为当发送消息时，可能连接等原因导致此次消息没有成功发出，
     * 那么暂存至此列表中，以备下次连接恢复时，再次由本类自动完成发送，从而确保消息不丢并确保送达。
     */
    protected ConcurrentLinkedQueue<String[]> publishTrayAgainCache = new ConcurrentLinkedQueue<String[]>();
    /**
     * 是否支持再次发送：true表示本地生产者发送失败时（比如连接MQ服务器不成功等情况）暂时缓存到本地内
     * 存 publishTrayAgainCache 中，等到与MQ中间件的连接成功时，自动再次尝试发送此缓存队列，从而保
     * 证生产者本来要发生送的消息在异常发生的情况下能再次发送直到成功。
     */
    protected boolean publishTrayAgainEnable = false;

    /**
     * 本类中消费者收到的消息通过此观察者进行回调通知
     */
    protected Observer consumerObserver = null;

    /**
     * 发出消息的字符编码格式
     */
    protected String encodeCharset = null;
    /**
     * 收到消息的字符解码格式
     */
    protected String decodeCharset = null;
    protected String mqURI = null;
    /**
     * 生产者消息中转队列名：本类是此队列的生产者，会将消息发送至此
     */
    protected String publishToQueue = null;
    /**
     * 消费者消息中转队列名：本类是此队列的消费者，将从其中读取消息
     */
    protected String consumFromQueue = null;

    /**
     * TAG for log
     */
    protected String TAG = null;

    public MQProvider(String mqURI, String publishToQueue, String consumFromQueue, String TAG, boolean publishTrayAgainEnable) {
        this(mqURI, publishToQueue, consumFromQueue, null, null, TAG, publishTrayAgainEnable);
    }

    public MQProvider(String mqURI, String publishToQueue, String consumFromQueue
            , String encodeCharset, String decodeCharset, String TAG
            , boolean publishTrayAgainEnable) {
        this.mqURI = mqURI;
        this.publishToQueue = publishToQueue;
        this.consumFromQueue = consumFromQueue;
        this.encodeCharset = encodeCharset;
        this.decodeCharset = decodeCharset;
        this.TAG = TAG;

        if (this.mqURI == null)
            throw new IllegalArgumentException("[" + TAG + "]无效的参数mqURI ！");

        if (this.publishToQueue == null && this.consumFromQueue == null)
            throw new IllegalArgumentException("[" + TAG + "]无效的参数，publishToQueue和" +
                    "consumFromQueue至少应设置其一！");

        if (this.encodeCharset == null || this.encodeCharset.trim().length() == 0)
            this.encodeCharset = DEFAULT_ENCODE_CHARSET;
        if (this.decodeCharset == null || this.decodeCharset.trim().length() == 0)
            this.decodeCharset = DEFAULT_DECODE_CHARSET;

        init();
    }

    protected boolean init() {
        String uri = this.mqURI;
        _factory = new ConnectionFactory();

        try {
            _factory.setUri(uri);
        } catch (Exception e) {
            logger.error("[" + TAG + "] - 【严重】factory.setUri()时出错，Uri格式不对哦，uri=" + uri, e);
            return false;
        }

        ////设置网络异常重连
        _factory.setAutomaticRecoveryEnabled(true);
        //设置重新声明交换器，队列等信息。
        _factory.setTopologyRecoveryEnabled(false);
        //设置 没5s ，重试一次
        _factory.setNetworkRecoveryInterval(5000);
        //心跳间隔
        _factory.setRequestedHeartbeat(30);
        //设置tcp链接超时时间
        _factory.setConnectionTimeout(30 * 1000);

        return true;
    }

    protected Connection tryGetConnection() {
        if (_connection == null) {
            try {
                _connection = _factory.newConnection();
                _connection.addShutdownListener(new ShutdownListener() {
                    public void shutdownCompleted(ShutdownSignalException cause) {
                        logger.warn("[" + TAG + "] - 连接已经关闭了。。。。【NO】");
                    }
                });

                ((Recoverable) _connection).addRecoveryListener(new RecoveryListener() {
                    @Override
                    public void handleRecovery(Recoverable arg0) {
                        logger.info("[" + TAG + "] - 连接已成功自动恢复了！【OK】");

                        start();
                    }
                });
            } catch (Exception e) {
                logger.error("[" + TAG + "] - 【NO】getConnection()时出错了，原因是：" + e.getMessage(), e);
                _connection = null;
                return null;
            }
        }

        return _connection;
    }

    public void start() {
        if (startRunning)
            return;

        try {
            if (_factory != null) {
                Connection conn = tryGetConnection();
                if (conn != null) {
                    whenConnected(conn);
                } else {
                    logger.error("[" + TAG + "-↑] - [start()中]【严重】connction还没有准备好" +
                            "，conn.createChannel()失败，start()没有继续！(原因：connction==null)【5秒后重新尝试start】");

                    timerForStartAgain.schedule(new TimerTask() {
                        public void run() {
                            start();
                        }
                    }, 5 * 1000);
                }
            } else {
                logger.error("[" + TAG + "-↑] - [start()中]【严重】factory还没有准备好，start()失败！(原因：factory==null)");
            }
        } finally {
            startRunning = false;
        }
    }

    protected void whenConnected(Connection conn) {
        this.startPublisher(conn);
        this.startWorker(conn);
    }

    protected void startPublisher(Connection conn) {
        if (conn != null) {
            if (_pubChannel != null && _pubChannel.isOpen()) {
                try {
                    _pubChannel.close();
                } catch (Exception e) {
                    logger.warn("[" + TAG + "-↑] - [startPublisher()中]pubChannel.close()时发生错误。", e);
                }
            }

            try {
                _pubChannel = conn.createChannel();

                logger.info("[" + TAG + "-↑] - [startPublisher()中] 的channel成功创建了，" +
                        "马上开始循环publish消息，当前数组队列长度：N/A！【OK】");//"+offlinePubQueue.size()+"！【OK】");

                //队列名称
                String queue = this.publishToQueue;
                //队列是否持久化.false:队列在内存中,服务器挂掉后,队列就没了;
                // true:服务器重启后,队列将会重新生成.注意:只是队列持久化,不代表队列中的消息持久化!!!!
                boolean durable = true;
                //队列是否专属,专属的范围针对的是连接,也就是说,一个连接下面的多个信道是可见的.
                // 对于其他连接是不可见的.连接断开后,该队列会被删除.注意,不是信道断开,是连接断开.并且,就算设置成了持久化,也会删除.
                boolean exclusive = false;
                //如果所有消费者都断开连接了,是否自动删除.如果还没有消费者从该队列获取过消息或者监听该队列,那么该队列不会删除.
                // 只有在有消费者从该队列获取过消息后,该队列才有可能自动删除(当所有消费者都断开连接,不管消息是否获取完)
                boolean autoDelete = false;

                //Message TTL : 消息生存期
                //Auto expire : 队列生存期
                //Max length : 队列可以容纳的消息的最大条数
                //Max length bytes : 队列可以容纳的消息的最大字节数
                //Overflow behaviour : 队列中的消息溢出后如何处理
                //Dead letter exchange : 溢出的消息需要发送到绑定该死信交换机的队列
                //Dead letter routing key : 溢出的消息需要发送到绑定该死信交换机,并且路由键匹配的队列
                //Maximum priority : 最大优先级
                //Lazy mode : 懒人模式
                //Master locator :
                //创建一个消息队列
                AMQP.Queue.DeclareOk qOK = _pubChannel.queueDeclare(queue, durable, exclusive, autoDelete, null);

                logger.info("[" + TAG + "-↑] - [startPublisher中] Queue[当前队列消息数：" + qOK.getMessageCount()
                        + ",消费者：" + qOK.getConsumerCount() + "]已成功建立，Publisher初始化成功，"
                        + "消息将可publish过去且不怕丢失了。【OK】(当前暂存数组长度:N/A)");//"+offlinePubQueue.size()+")");

                if (publishTrayAgainEnable) {
                    while (publishTrayAgainCache.size() > 0) {
                        String[] m = publishTrayAgainCache.poll();
                        if (m != null && m.length > 0) {
                            logger.debug("[" + TAG + "-↑] - [startPublisher()中] [...]在channel成功创建后，正在publish之前失败暂存的消息 m[0]=" + m[0]
                                    + "、m[1]=" + m[1] + ",、m[2]=" + m[2] + "，[当前数组队列长度：" + publishTrayAgainCache.size() + "]！【OK】");
                            publish(m[0], m[1], m[2]);
                        } else {
                            logger.debug("[" + TAG + "-↑] - [startPublisher()中] [___]在channel成功创建后，" +
                                    "当前之前失败暂存的数据队列已为空，publish没有继续！[当前数组队列长度：" + publishTrayAgainCache.size() + "]！【OK】");
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("[" + TAG + "-↑] - [startPublisher()中] conn.createChannel()或pubChannel.queueDeclare()" +
                        "出错了，本次startPublisher没有继续！", e);
            }
        } else {
            logger.error("[" + TAG + "-↑] - [startPublisher()中]【严重】connction还没有准备好" +
                    "，conn.createChannel()失败！(原因：connction==null)");
        }
    }

    public boolean publish(String message) {
        return this.publish("", this.publishToQueue, message);
    }

    protected boolean publish(String exchangeName, String routingKey, String message) {
        boolean ok = false;

        try {
            _pubChannel.basicPublish(exchangeName, routingKey
                    , MessageProperties.PERSISTENT_TEXT_PLAIN
                    , message.getBytes(this.encodeCharset));
            logger.info("[" + TAG + "-↑] - [startPublisher()中] publish()成功了 ！(数据:"
                    + exchangeName + "," + routingKey + "," + message + ")");
            ok = true;
        } catch (Exception e) {
            if (publishTrayAgainEnable) {
                publishTrayAgainCache.add(new String[]{exchangeName, routingKey, message});
            }

            logger.error("[" + TAG + "-↑] - [startPublisher()中] publish()时Exception了，" +
                    "原因：" + e.getMessage() + "【数据[" + exchangeName + "," + routingKey + "," + message + "]已重新放回数组首位" +
                    "，当前数组长度：N/A】", e);//"+offlinePubQueue.size()+"】", e);
        }
        return ok;
    }

    protected void startWorker(Connection conn) {
        if (this.retryWorkerRunning)
            return;

        try {
            if (conn != null) {
                final Channel resumeChannel = conn.createChannel();

                String queueName = this.consumFromQueue;//queue name

                DefaultConsumer dc = new DefaultConsumer(resumeChannel) {
                    @Override
                    public void handleDelivery(String consumerTag, Envelope envelope,
                                               AMQP.BasicProperties properties, byte[] body) throws IOException {
                        String routingKey = envelope.getRoutingKey();
                        String contentType = properties.getContentType();

                        long deliveryTag = envelope.getDeliveryTag();

                        logger.info("[" + TAG + "-↓] - [startWorker()中] 收到一条新消息(routingKey="
                                + routingKey + ",contentType=" + contentType + ",consumerTag=" + consumerTag
                                + ",deliveryTag=" + deliveryTag + ")，马上开始处理。。。。");

                        boolean workOK = work(body);
                        if (workOK) {
                            resumeChannel.basicAck(deliveryTag, false);
                        } else {
                            resumeChannel.basicReject(deliveryTag, true);
                        }
                    }
                };

                boolean autoAck = false;
                resumeChannel.basicConsume(queueName, autoAck, dc);

                logger.info("[" + TAG + "-↓] - [startWorker()中] Worker已经成功开启并运行中...【OK】");

            } else {
                throw new Exception("[" + TAG + "-↓] - 【严重】connction还没有准备好，conn.createChannel()失败！(原因：connction==null)");
            }
        } catch (Exception e) {
            logger.error("[" + TAG + "-↓] - [startWorker()中] conn.createChannel()或Consumer操作时" +
                    "出错了，本次startWorker没有继续【暂停5秒后重试startWorker()】！", e);

            this.timerForRetryWorker.schedule(new TimerTask() {
                public void run() {
                    startWorker(MQProvider.this._connection);
                }
            }, 5 * 1000);
        } finally {
            retryWorkerRunning = false;
        }
    }

    /**
     * 处理接收到的消息。子类需要重写本方法以便实现自已的处理逻辑，本方法默认只作为log输出使用！
     * 特别注意：本方法一旦返回false则消息将被MQ服务器重新放入队列， 请一定注意false是你需要的，不然消息会重复哦。
     *
     * @param contentBody 从MQ服务器取到的消息内容byte数组
     * @return true表示此消息处理成功(本类将回复ACK给MQ服务端 ， 服务端队列中会交此消息正常删除)，
     * 否则不成功(本类将通知MQ服务器将该条消息重新放回队列，以备下次再次获取)。
     */
    protected boolean work(byte[] contentBody) {
        try {
            String msg = new String(contentBody, this.decodeCharset);
            logger.info("[" + TAG + "-↓] - [startWorker()中] Got msg：" + msg);
            return true;
        } catch (Exception e) {
            logger.warn("[" + TAG + "-↓] - [startWorker()中] work()出现错误，错误将被记录：" + e.getMessage(), e);
//			return false;
            return true;
        }
    }
}
