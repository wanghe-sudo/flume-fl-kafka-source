package org.wangh.flume.interceptor;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wangh.flume.utils.GzFileUtils;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @Author wanghe
 * @Date 2022/10/25
 * @DESC 多个agent负载均衡, 使用拦截器处理每一个event
 */
public class TagInterceptor implements Interceptor {

    /**
     * LOG
     */
    private static final Logger LOG = LoggerFactory.getLogger(TagInterceptor.class);

    /**
     * 每次读取文件的大小
     */
    private static int readPosition;

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        byte[] body = event.getBody();

        String filStr = new String(body);

        String[] gzNnames = filStr.split(",");

        StringBuilder builder = new StringBuilder();
        for (String gzName : gzNnames) {
            if (StringUtils.isEmpty(gzName) || !gzName.endsWith("gz")) {
                continue;
            }
            File gzFile = new File(gzName);
            if (!gzFile.exists()) {
                continue;
            }
            LOG.info("待解析的文件:{}", gzName);
            String context = GzFileUtils.readFileToStr(gzFile, readPosition);
            builder.append(context).append("\\n");
            boolean delete = gzFile.delete();
            if (delete) {
                LOG.info("删除已读文件成功:{}", gzName);
            } else {
                LOG.error("删除已读文件失败:{}", gzName);
            }
        }
        event.setBody(builder.toString().getBytes());
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        return events.stream().map(this::intercept).collect(Collectors.toList());
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new TagInterceptor();
        }

        public void configure(Context context) {
            readPosition = context.getInteger("filePosition", 20480);
            LOG.info("read position :{}", readPosition);
        }
    }
}
