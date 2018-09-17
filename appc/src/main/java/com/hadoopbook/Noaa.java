package com.hadoopbook;

/**
 * <pre>
 *
 * </pre>
 * Author: 王俊超
 * Date: 2018-09-16 21:06
 * Blog: http://blog.csdn.net/derrantcm
 * Github: https://github.com/wang-jun-chao
 * All Rights Reserved !!!
 */
public class Noaa {
    public static void main(String[] args) throws Exception {

        FileThread file = new FileThread();
        for (int i = 1929; i <= 2000; i++) {

//			new FileThread(new Integer(i).toString()).start();
            file.setYear(new Integer(i).toString());
            file.run();

        }
    }

}
