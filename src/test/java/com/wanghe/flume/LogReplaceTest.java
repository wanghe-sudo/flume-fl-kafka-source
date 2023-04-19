package com.wanghe.flume;

import org.junit.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Author wanghe
 * @Date 2022/10/20
 * @DESC 测试使用正则表达式处理log中的时间信息
 */
public class LogReplaceTest {
    /**
     * 多两个|,是为了增加匹配的精度,同时,可以将相同的日期一起替换,提高效率
     */
    private static final Pattern TIME_REX = Pattern.compile("\\|\\d{14}\\|");

//    private static final String TIME_REX = "\\|\\d{14}\\|";

    @Test
    public void replace() {

        String allLine = "170.247.139.70|devices.shopsavvy.mobi|20221020151732|45.59.68.125|0|1|||192.168.21.110\n" +
                "170.214.250.24|homesforhope.scmp.com|20221020151732|116.78.85.23|0|1|||192.168.21.110\n" +
                "170.27.190.155|se066.com|20221020151732|24.46.14.120|0|1|||192.168.21.110\n" +
                "170.24.128.95|shop67312385.taobao.com|20221020151732|53.3.102.8|0|1|||192.168.21.110\n" +
                "170.232.197.165|www.dfzeg.com|20221020151732|32.36.20.79|0|1|||192.168.21.110\n" +
                "170.204.184.186|diaifei.taobao.com|20221020151732|6.64.123.60|0|1|||192.168.21.110\n" +
                "170.196.104.94|dapaihome.mall.taobao.com|20221020151732|4.48.12.123|0|1|||192.168.21.110\n" +
                "170.113.177.155|www.612088.co.cc|20221020151732|7.29.12.31|0|1|||192.168.21.110\n" +
                "170.185.233.200|manchen.tmall.com|20221020151732|23.6.18.30|0|1|||192.168.21.110\n" +
                "170.135.243.105|shop34451166.taobao.com|20221020151732|77.57.118.12|0|1|||192.168.21.110\n" +
                "170.40.23.203|nullmx.zhongtai.com|20221020151732|49.111.100.61|0|1|||192.168.21.110\n" +
                "170.2.227.52|hengyuan66666.en.busytrade.com|20221020151732|13.57.124.63|0|1|||192.168.21.110\n" +
                "170.73.91.59|shop36248913.taobao.com|20221020151732|64.1.126.93|0|1|||192.168.21.110\n" +
                "170.185.111.227|mail.rubyicl.com.hk|20221020151833|43.8.107.29;30.43.87.16;122.112.48.110|0|1|||192.168.21.110\n" +
                "170.166.168.161|searchassist.babylon.com|20221020151732|66.8.49.53|0|1|||192.168.21.110\n" +
                "170.78.8.215|changshaditu.51yala.com|20221020151732|57.12.15.67|0|1|||192.168.21.110\n" +
                "170.153.218.96|www.yanzhaomen.net|20221020151732|120.33.0.122|0|1|||192.168.21.110\n" +
                "170.153.195.208|shenzhenxiange.b2b.hc360.com|20221020151732|59.114.7.127|0|1|||192.168.21.110\n" +
                "170.35.185.145|s25.lumfile.com|20221020151732|42.25.63.50|0|1|||192.168.21.110\n" +
                "170.98.63.97|www.gui8.com|20221020151732|109.52.123.104|0|1|||192.168.21.110\n" +
                "170.91.226.250|www.ystjq.com|20221020151732|43.25.126.32|0|1|||192.168.21.110\n" +
                "170.220.65.3|artwuyouming.t.sohu.com|20221020151732|78.7.123.18|0|1|||192.168.21.110\n" +
                "170.214.76.65|11.178.179.60.zz.countries.nerd.dk|20221020151732|15.34.70.81|0|1|||192.168.21.110\n" +
                "170.52.54.21|supermiu.net|20221020151732|13.44.93.47|0|1|||192.168.21.110\n" +
                "170.185.186.146|c2.www30.babidou.com|20221020151732|33.117.46.76|0|1|||192.168.21.110";
        Matcher matcher = null;
        int i = 0;
        while (true) {
            matcher = TIME_REX.matcher(allLine);
            if (matcher.find()) {
                i++;
                String group = matcher.group();
                group = group.replaceAll("\\|", "");
                String year = group.substring(0, 4);
                String month = group.substring(4, 6);
                String day = group.substring(6, 8);
                String hour = group.substring(8, 10);
                String minute = group.substring(10, 12);
                String timeFormat = String.format(year + "-" + month + "-" + day + " " + hour + ":" + minute + ":00");
                allLine = allLine.replaceAll(group, timeFormat);
            } else {
                break;
            }
        }
        // 最小遍历数
        System.out.println(i);
        System.out.println(allLine);
    }
}
