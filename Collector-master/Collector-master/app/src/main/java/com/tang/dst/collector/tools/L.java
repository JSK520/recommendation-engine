package com.tang.dst.collector.tools;

import android.util.Log;

/**
 * Created by D.S.T on 16/12/25.
 */
public class L {

    private static final String TAG = "TANG.CN ---- okHttp";
    private static boolean debug = true;

    public static void e(String msg) {
        if(debug)
            Log.e(TAG, msg);
    }
}
