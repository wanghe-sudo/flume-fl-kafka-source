package org.wangh.flume.constant;

import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;

/**
 * @Author wanghe
 * @Date 2022/10/27
 * @DESC
 */
public class Constant {

    /**
     * 处理日志中的时间信息正则
     */
    public static final Pattern TIME_REX = Pattern.compile("\\|\\d{14}\\|");

    /**
     * 非标准化格式化时间
     */
    public static final DateTimeFormatter IMNORM_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    /**
     * 非标准化格式化时间
     */
    public static final DateTimeFormatter IMNORM_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    /**
     * 标准化格式化时间
     */
    public static final DateTimeFormatter NORM_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:00");
}
