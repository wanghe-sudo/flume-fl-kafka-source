package org.wangh.flume.source;

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
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

/**
 * @Author wanghe
 * @Date 2022/10/17
 * @DESC 将每一个gz日志包读取并将内容写入channle
 */
public class TagSource extends AbstractSource implements Configurable, PollableSource {

    /**
     * LOG
     */
    private static final Logger LOG = LoggerFactory.getLogger(TagSource.class);


    private List<File> gzFiles = new ArrayList<>(500);
    /**
     * 监控路径
     */
    private File listenDir;

    /**
     * 每次读取文件的大小
     */
    private int readPosition;

    /**
     * 文件列表.
     */
    private ConcurrentLinkedQueue<String> gzNameQueue = new ConcurrentLinkedQueue<String>();

    /**
     * 睡眠时间
     */
    private long sleepTime = 1000L;

    /**
     * 最长睡眠时间
     */
    private long maxSleepTime = 60000L;


    /**
     * process 核心方法:每次只取一个进行处理
     *
     * @return Status
     * @throws EventDeliveryException
     */
    @Override
    public Status process() throws EventDeliveryException {
        LocalDateTime now = LocalDateTime.now();
        String dateStr = now.format(Constant.IMNORM_DATE_FORMATTER);
        File gzDir = new File(listenDir.getAbsoluteFile() + File.separator + dateStr);

        if (!gzDir.exists() || !gzDir.isDirectory()) {
            LOG.error("监控的路径不存在:{}", gzDir.getAbsolutePath());
            sleep(60000L);
            return Status.BACKOFF;
        }
        if (gzFiles.size() > 0) {
            LOG.info("待处理的文件数:{}", gzFiles.size());
            File gzFile = gzFiles.get(0);
            // 读取文件
            byte[] bytes = GzFileUtils.readFileToByte(gzFile, readPosition);
            // 写入event
            SimpleEvent event = new SimpleEvent();
            event.setBody(bytes);
            LOG.info("读取文件:{},写入channle,删除文件", gzFile.getName());
            boolean delete = gzFile.delete();
            if (!delete) {
                LOG.error("删除文件失败");
            }
            gzFiles.remove(0);
            //将数据发送到channel
            getChannelProcessor().processEvent(event);
            return Status.READY;
        } else {
            this.gzFiles.addAll(Arrays.stream(Objects.requireNonNull(gzDir.listFiles(file -> file.canRead()
                    && file.isFile()
                    // 筛选出最后修改时间已经过去5秒的文件
                    && (now.toInstant(ZoneOffset.of("+8")).toEpochMilli() - file.lastModified()) > 5000
                    && file.getName().endsWith("gz")))).collect(Collectors.toList()));

            if (this.gzFiles.size() == 0) {
                LOG.info("待读取文件:0个");
                sleep(1000L);
            }
            return Status.BACKOFF;
        }
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

        this.readPosition = context.getInteger("filePosition", 10240);
        LOG.info("read position :{}", readPosition);
    }

    /**
     * 睡眠.
     */
    private void sleep(long sleepTime) {
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            //throw new RuntimeException(e);
            LOG.info("Thread.sleep error:{}", e.getMessage());
        }
    }
}
