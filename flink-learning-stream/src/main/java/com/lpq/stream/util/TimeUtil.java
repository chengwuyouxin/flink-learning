package com.lpq.stream.util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author liupengqiang
 * @date 2020/5/22
 */
public class TimeUtil {
    public static String unix2Str(Long timestamp){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss.SSS");
        return sdf.format(new Date(timestamp));
    }
}
