package org.wangh.flume.utils;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wangh.flume.constant.Constant;
import org.wangh.flume.source.TagSource;

import java.io.*;
import java.nio.file.Files;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.regex.Matcher;

/**
 * @Author wanghe
 * @Date 2022/10/25
 * @DESC
 */
public class GzFileUtils {

    /**
     * LOG
     */
    private static final Logger LOG = LoggerFactory.getLogger(TagSource.class);


    /**
     * 格式化时间
     *
     * @param time
     * @return
     */
    public static String formatFileTime(LocalDateTime time) {
        return time.format(Constant.IMNORM_TIME_FORMATTER);
    }

    /**
     * 读文件到channle
     *
     * @param file
     * @return
     */
    public static byte[] readFileToByte(File file, int readPosition) {
        String context = readFileToStr(file, readPosition);
        return context.getBytes();
    }

    /**
     * 读文件到channle
     *
     * @param file
     * @return
     */
    public static String readFileToStr(File file, int readPosition) {
        LocalDateTime starTime = LocalDateTime.now();
        InputStream inputStream = null;
        GzipCompressorInputStream gzipCompressorInputStream = null;
        BufferedReader bufferedReader = null;
        StringBuilder builder = new StringBuilder();
        try {
            inputStream = Files.newInputStream(file.toPath());
            gzipCompressorInputStream = new GzipCompressorInputStream(inputStream);
            int boot = 0;
            char[] chars = new char[readPosition];
            bufferedReader = new BufferedReader(new InputStreamReader(gzipCompressorInputStream));
            while ((boot = bufferedReader.read(chars)) != -1) {
                String s = new String(chars, 0, boot);
                builder.append(s);
            }
            LocalDateTime ensTime = LocalDateTime.now();
            long seconds = Duration.between(starTime, ensTime).toMillis();
            LOG.info("读取文件:{},大小:{}M,用时:{}毫秒", file.getName(), file.length() / 1024 / 1024, seconds);
        } catch (IOException e) {
            LOG.error("读取文件失败", e);
        } finally {
            if (null != bufferedReader) {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    LOG.error("关闭bufferedReader异常:{}", e.getMessage());
                }
            }
            if (null != gzipCompressorInputStream) {
                try {
                    gzipCompressorInputStream.close();
                } catch (IOException e) {
                    LOG.error("关闭gzipCompressorInputStream异常:{}", e.getMessage());
                }
            }
            if (null != inputStream) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    LOG.error("关闭inputStream异常:{}", e.getMessage());
                }
            }
        }
        String context = formatTimeInfo(builder.toString());
        // LOG.debug("处理后的字符串:{}", context);
        return context;
    }

    /**
     * 格式化time信息
     *
     * @param context
     * @return
     */
    public static String formatTimeInfo(String context) {
        Matcher matcher = null;
        while (true) {
            matcher = Constant.TIME_REX.matcher(context);
            if (matcher.find()) {
                String group = matcher.group();
                group = group.replaceAll("\\|", "");
                LocalDateTime parse = LocalDateTime.parse(group, Constant.IMNORM_TIME_FORMATTER);
                String timeInfo = parse.format(Constant.NORM_TIME_FORMATTER);
                String timeFormat = String.format(timeInfo);
                context = context.replaceAll(group, timeFormat);
            } else {
                break;
            }
        }
        LOG.debug("处理后的字符串:{}", context);
        return context;
    }

}
