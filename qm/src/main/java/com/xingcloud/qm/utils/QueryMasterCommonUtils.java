package com.xingcloud.qm.utils;

import static com.xingcloud.qm.utils.QueryMasterConstant.META_KEY;
import static com.xingcloud.qm.utils.QueryMasterConstant.SIZE_KEY;

import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;

public class QueryMasterCommonUtils {
    public static void printRelations( MapWritable... data ) {

        LongWritable lw = null;
        ArrayWritable aw = null;
        for( MapWritable mw: data ) {
            System.out.println("===================================");
            lw = (LongWritable) mw.get(SIZE_KEY);
            if (lw == null) {
                System.out.println("[SIZE]: No size.");
            } else {
                System.out.println("[SIZE]: " + lw.get());
            }

            aw = (ArrayWritable) mw.get(META_KEY);
            if (aw == null) {
                System.out.println("[SIZE]: No meta.");
            } else {
                System.out
                        .println("[META]: " + Arrays.toString(aw.toStrings()));
            }
            System.out.println("[DATA]:");

            for( long i = 0; i < lw.get(); i++ ) {
                aw = (ArrayWritable) mw.get(new LongWritable(i));
                System.out.println("\t" + i + "\t"
                        + Arrays.toString(aw.toStrings()));
            }
        }
    }
}
