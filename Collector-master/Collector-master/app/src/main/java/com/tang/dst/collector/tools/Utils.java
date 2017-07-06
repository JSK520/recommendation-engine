package com.tang.dst.collector.tools;

import android.app.Activity;
import android.app.AlertDialog;
import android.app.Dialog;
import android.content.ClipboardManager;
import android.content.Context;
import android.content.DialogInterface;
import android.text.format.Time;
import android.view.WindowManager;
import android.widget.TextView;
import android.widget.Toast;

import com.tang.dst.collector.R;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;

public class Utils {
    public static String getTextFromStream(InputStream is) {

        byte[] b = new byte[1024];
        int len = 0;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            while ((len = is.read(b)) != -1) {
                bos.write(b, 0, len);
            }
            String text = new String(bos.toByteArray());
            bos.close();
            return text;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 字符编码utf-8
     *
     * @param s
     * @return
     */
    public static String handleString(String s) {
        try {
            byte bb[] = s.getBytes("GB2312");
            s = new String(bb);
        } catch (Exception ee) {

        }
        return s;
    }

    public static int getStatusBarHeight(Context context) {
        Class<?> c = null;
        Object obj = null;
        Field field = null;
        int x = 0, statusBarHeight = 0;
        try {
            c = Class.forName("com.android.internal.R$dimen");
            obj = c.newInstance();
            field = c.getField("status_bar_height");
            x = Integer.parseInt(field.get(obj).toString());
            statusBarHeight = context.getResources().getDimensionPixelSize(x);
        } catch (Exception e1) {
            e1.printStackTrace();
        }
        return statusBarHeight;
    }

    /*
    功能：得到剪贴板管理器
     */
    public static void copy(String content, Context context) {
        ClipboardManager cmb = (ClipboardManager) context
                .getSystemService(Context.CLIPBOARD_SERVICE);
        cmb.setText(content.trim());
    }

    public static String getTime(){
        Time t = new Time();
        t.setToNow(); // 取得系统时间。
        int year = t.year;
        int month = t.month + 1;
        int date = t.monthDay;
        int hour = t.hour;
        int min = t.minute;
        int sec = t.second;
        String time = "";
        if (month < 10) {
            time = year + "/0" + month + "/" + date + " " + hour + ":" + min + ":" + sec;
        } else if (date < 10) {
            time = year + "/" + month + "/0" + date + " " + hour + ":" + min + ":" + sec;
        } else if (month < 10 && date < 10) {
            time = year + "/0" + month + "/0" + date + " " + hour + ":" + min + ":" + sec;
        } else {
            time = year + "/" + month + "/" + date + " " + hour + ":" + min + ":" + sec;
        }
        return time;
    }

    /**
     * 设置添加屏幕的背景透明度
     *
     * @param bgAlpha
     */
    public static void backgroundAlpha(Activity activity, float bgAlpha) {
        WindowManager.LayoutParams lp = activity.getWindow().getAttributes();
        lp.alpha = bgAlpha; //0.0-1.0
        activity.getWindow().setAttributes(lp);
    }

    public static Dialog test(final Activity activity,final TextView back) {
        back.setBackgroundResource(R.color.gray);
        Dialog dialog = new AlertDialog.Builder(activity)
                .setOnDismissListener(new DialogInterface.OnDismissListener() {
                    @Override
                    public void onDismiss(DialogInterface dialogInterface) {
                        back.setBackgroundResource(R.color.white);
                    }
                })
                .setItems(new String[] { "复制文本" }, new AlertDialog.OnClickListener() {
                    public void onClick(DialogInterface dialog, int which) {
                        String string = back.getText().toString();
                        Utils.copy(string,activity);
                        Toast.makeText(activity,"文本已复制",Toast.LENGTH_SHORT).show();
                    }
                }).create();
        dialog.setCanceledOnTouchOutside(true);
        dialog.show();
        return dialog;
    }
}
