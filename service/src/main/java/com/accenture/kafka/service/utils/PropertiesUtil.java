package com.accenture.kafka.service.utils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.Properties;

/**
 * Created by THINK on 2016/11/22.
 */
public class PropertiesUtil {
    public static void store(Properties p, OutputStream out, String comments) throws IOException {
        Properties ps = new Properties();
        Enumeration<?> enumeration = p.propertyNames();

        while (enumeration.hasMoreElements()) {
            Object o = enumeration.nextElement();
            ps.put(o, p.get(o).toString());
        }

        try {
            ps.store(out, comments);
        } finally {
            out.flush();
            out.close();
        }
    }
}
