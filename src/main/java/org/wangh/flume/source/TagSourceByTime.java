package org.wangh.flume.source;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wangh.flume.constant.Constant;
import org.wangh.flume.utils.GzFileUtils;

import java.io.File;
import java.time.LocalDateTime;

/**
 * @Author wanghe
 * @Date 2022/10/21
 * @DESC
 */
public class TagSourceByTime extends AbstractSource implements Configurable, PollableSource {

    /**
     * LOG
     */
    private static Logger LOG = LoggerFactory.getLogger(TagSourceByTime.class);

    /**
     * 根据time查询包含该时间的gz文件
     */
    private static LocalDateTime time;

    /**
     * 延后时间
     */
    private static final Long TIME_LATE = -5L;

    /**
     * 监控路径
     */
    private static File listenDir;

    /**
     * 每次读取文件的大小
     */
    private int readPosition;

    @Override
    public Status process() throws EventDeliveryException {
/*  不建议睡1秒钟,读取文件还需要时间,后面会越来越慢      try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }*/

        // 给一个10s的延迟,保证time plus后,仍然比now慢10s
        LocalDateTime now = LocalDateTime.now();
        String dateStr = now.format(Constant.IMNORM_DATE_FORMATTER);
        File gzDir = new File(listenDir.getAbsoluteFile() + File.separator + dateStr);


        if (now.plusSeconds(TIME_LATE).isBefore(time)) {
            LOG.debug("延时{}读取时间未到,返回BACKOFF", TIME_LATE);
            return Status.BACKOFF;
        }

        if (gzDir.exists() && gzDir.isDirectory()) {
            String gzNameTime = GzFileUtils.formatFileTime(time);
            LOG.info("gzNameTime:{}", gzNameTime);
            // 一个时间一个文件
            File[] gzFiles = gzDir.listFiles((dir, name) -> name.contains(gzNameTime) && name.endsWith("gz"));
            if (null == gzFiles || 0 == gzFiles.length) {
                LOG.info("读取{}时刻,gz文件不存在,时间加1s:{}", time, gzFiles);
                time = time.plusSeconds(1L);
                return Status.BACKOFF;
            }
            LOG.info("文件数量:{}", gzFiles.length);
            File gzFile = gzFiles[0];

            // 读取文件
            byte[] bytes = GzFileUtils.readFileToByte(gzFile, readPosition);

            // 写入event
            SimpleEvent event = new SimpleEvent();

            event.setBody(bytes);

            // 更新时间
            time = time.plusSeconds(1L);

            // 删除已读取文件
            boolean delete = gzFile.delete();
            if (delete) {
                LOG.info("删除已读取文件成功:{}", gzFile);
            } else {
                LOG.info("删除已读取文件失败:{}", gzFile);
            }

            LOG.info("写入event成功,发送数据到channle");

            //将数据发送到channel
            getChannelProcessor().processEvent(event);
            // 返回状态
            return Status.READY;
        }
        return Status.BACKOFF;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    @Override
    public void configure(Context context) {
        String filePath = context.getString("filePath");
        listenDir = new File(filePath);

        readPosition = context.getInteger("filePosition", 20480);
        LOG.info("read position :{}", readPosition);

        String startTime = context.getString("startTime", "");
        if (StringUtils.isEmpty(startTime)) {
            time = LocalDateTime.now();
        }
        try {
            LocalDateTime parse = LocalDateTime.parse(startTime, Constant.IMNORM_TIME_FORMATTER);
            time = parse;
        } catch (Exception e) {
            LOG.warn("解析配置文件时间对象失败:{},使用当前时间作为开始时间:{}", startTime, time);
        }
        LOG.info("初始化自定义source启动时间:{},从当前时间开始监控gz文件新增", time);
    }
}
