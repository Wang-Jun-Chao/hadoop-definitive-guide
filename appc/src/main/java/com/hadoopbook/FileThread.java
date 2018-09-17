package com.hadoopbook;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

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
public class FileThread extends Thread {
    String year;

    public FileThread() {
    }

    public FileThread(String year) {
        this.year = year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    @Override
    public void run() {

        InputStream input = null;
        FileOutputStream output = null;

        try {
            URL url = new URL("ftp://ftp.ncdc.noaa.gov/pub/data/gsod/" + year + "/gsod_" + year + ".tar");
            input = url.openStream();

            output = new FileOutputStream(new File("F:/gsod/gsod_" + year + ".tar"));
            int ls = 0;
            byte b[] = new byte[204800];

            while ((ls = input.read(b, 0, 204800)) > -1) {
                output.write(b, 0, ls);
                output.flush();
            }
            output.flush();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (output != null) {
                    output.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                if (input != null) {
                    input.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
