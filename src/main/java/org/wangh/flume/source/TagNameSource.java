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

import java.io.File;
import java.time.LocalDateTime;

/**
 * @Author wanghe
 * @Date 2022/10/25
 * @DESC 只读取新增gz文件名, 传入channle, 使用负载均衡写入多个agent,多任务进行读取文件并写入kafka
 */
public class TagNameSource extends AbstractSource implements Configurable, PollableSource {
    /**
     * LOG
     */
    private static final Logger LOG = LoggerFactory.getLogger(TagNameSource.class);

    /**
     * 根据time查询包含该时间的gz文件
     */
    private static LocalDateTime time;
    /**
     * 监控路径
     */
    private String listenDir;

    /**
     * 前缀
     */
    private String prefix;


    @Override
    public Status process() throws EventDeliveryException {
        LocalDateTime now = LocalDateTime.now();
        String dateStr = now.format(Constant.IMNORM_DATE_FORMATTER);
        File gzDir = new File(listenDir + File.separator + dateStr);
        if (!gzDir.exists() || !gzDir.isDirectory()) {
            LOG.error("监控的路径不存在:{}", gzDir.getAbsolutePath());
            return Status.BACKOFF;
        }

        File[] gzFils = gzDir.listFiles(gzFile -> {
            String name = gzFile.getName();
            return !name.startsWith(prefix) && name.endsWith("gz");
        });
        if (null == gzFils || gzFils.length == 0) {
            try {
                LOG.info("没有新增文件,等待");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return Status.BACKOFF;
        }
        LOG.info("文件数量:{}", gzFils.length);
        StringBuilder builder = new StringBuilder();
        for (File gzFile : gzFils) {
            String newName = gzFile.getParentFile() + File.separator + prefix + gzFile.getName();
            boolean rename = gzFile.renameTo(new File(newName));
            if (rename) {
                LOG.info("文件:{},新文件名为:{},修改成功", gzFile.getName(), newName);
                builder.append(newName).append(",");
            } else {
                LOG.warn("修改文件名失败...");
            }
        }
        LOG.info("已修改文件列表:{} 并写入channle", builder);
        SimpleEvent event = new SimpleEvent();
        event.setBody(builder.substring(0, builder.length() - 1).getBytes());
        getChannelProcessor().processEvent(event);
        return Status.READY;
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
        listenDir = context.getString("filePath");
        LOG.info("listenDir :{}", listenDir);

        this.prefix = context.getString("prefix", "YM_");

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
