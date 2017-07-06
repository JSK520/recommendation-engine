package com.tang.dst.collector.tools;

import android.app.Activity;
import android.os.Handler;
import android.os.Message;
import android.widget.Toast;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.tang.dst.collector.entry.Collection;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.util.List;

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

/**
 * Created by D.S.T on 16/12/11.
 */
public class HttpUtils {

    public HttpUtils(){

    }
    public static String getVersion(){
        String uri = "http://www.tangdesheng.cn:8080/version/version.jsp";
        URL url = null;
        StringBuilder response = null;
        try {
            url = new URL(uri);
            HttpURLConnection urlCon = (HttpURLConnection) url.openConnection();
            urlCon.setRequestMethod("GET");
            urlCon.setReadTimeout(8000);
            urlCon.setConnectTimeout(10000);
            urlCon.setDoOutput(true);
            urlCon.setDoInput(true);
            urlCon.setRequestProperty("Content-type", "UTF-8");
            InputStream in = urlCon.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            response = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
        } catch (ProtocolException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        String[] ver = response.toString().split(" ");
        return ver[0];
    }
    public static String getLink()throws Exception{
        String uri = "http://www.tangdesheng.cn:8080/version/version.jsp";
        URL url = null;
        StringBuilder response = null;
        try {
            url = new URL(uri);
            HttpURLConnection urlCon = (HttpURLConnection) url.openConnection();
            urlCon.setRequestMethod("GET");
            urlCon.setReadTimeout(8000);
            urlCon.setConnectTimeout(10000);
            urlCon.setDoOutput(true);
            urlCon.setDoInput(true);
            urlCon.setRequestProperty("Content-type", "UTF-8");
            InputStream in = urlCon.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            response = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
        } catch (ProtocolException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        String[] ver = response.toString().split(" ");
        return ver[1];
    }
    public static boolean isNewVersion(){

        return false;
    }
    public static int sendFeedBack(String feedback)throws Exception{
        String uri = "http://www.tangdesheng.cn/mobile/feedback.php?feedback="+feedback;
        URL url = null;
        try {
            url = new URL(uri);
            HttpURLConnection urlCon = (HttpURLConnection) url.openConnection();
            urlCon.setRequestMethod("GET");
            urlCon.setReadTimeout(8000);
            urlCon.setConnectTimeout(10000);
            urlCon.setDoOutput(true);
            urlCon.setDoInput(true);
            urlCon.setRequestProperty("Content-type", "UTF-8");
            urlCon.getInputStream();
            return 1;
        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        }
    }

    public static void sendMyFavorToServer(Collection c)throws Exception{
        String uri = "http://www.tangdesheng.cn/mobile/insert.php?title="+c.getTitle()+"&content="+c.getContent()
                +"&time="+c.getTime()+"&isfavor="+c.getIsfavor();
        URL url = null;
        try {
            url = new URL(uri);
            HttpURLConnection urlCon = (HttpURLConnection) url.openConnection();
            urlCon.setRequestMethod("GET");
            urlCon.setReadTimeout(8000);
            urlCon.setConnectTimeout(10000);
            urlCon.setDoOutput(true);
            urlCon.setDoInput(true);
            urlCon.setRequestProperty("Content-type", "UTF-8");
            urlCon.getInputStream();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void sendMyFavorToServer(String title,String content,String time,int isfavor)throws Exception{
        String uri = "http://www.tangdesheng.cn/mobile/insert.php?title="+title+"&content="+content
                +"&time="+time+"&isfavor="+isfavor;
        URL url = null;
        try {
            url = new URL(uri);
            HttpURLConnection urlCon = (HttpURLConnection) url.openConnection();
            urlCon.setRequestMethod("GET");
            urlCon.setReadTimeout(8000);
            urlCon.setConnectTimeout(10000);
            urlCon.setDoOutput(true);
            urlCon.setDoInput(true);
            urlCon.setRequestProperty("Content-type", "UTF-8");
            urlCon.getInputStream();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void saveSharedToNet(final Activity context, Collection col) throws IOException {
        OkHttpClient okHttpClient = new OkHttpClient();
        Request.Builder build = new Request.Builder();
        Request request = build.get().url("http://www.tangdesheng.cn:8080/collector/save?title="
                + col.getTitle() + "&content=" + col.getContent() + "&time=" + col.getTime() + "&isfavor="+col.getIsfavor()).build();

        Call call = okHttpClient.newCall(request);
        //call.execute();
        call.enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                L.e("onFailure: " + e.getMessage());
                e.printStackTrace();
            }

            @Override
            public void onResponse(Call call, Response response) throws IOException {
                final String res = response.body().string();
                context.runOnUiThread(new Runnable() {
                    @Override
                    public void run() {
                        Toast.makeText(context, res.trim(), Toast.LENGTH_SHORT).show();
                    }
                });

            }
        });
    }

    public void getFromNet(final Handler handler) throws IOException {
        OkHttpClient okHttpClient = new OkHttpClient();
        Request.Builder build = new Request.Builder();
        Request request = build.get().url("http://www.tangdesheng.cn:8080/collector/get").build();
        Call call = okHttpClient.newCall(request);

        //Response response = call.execute();
        call.enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                L.e("onFailure: " + e.getMessage());
                e.printStackTrace();
            }

            @Override
            public void onResponse(Call call, Response response) throws IOException {
                final String res = response.body().string();
                Gson gson = new Gson();
                final List<Collection> list;
                list = gson.fromJson(res.trim(),new TypeToken<List<Collection>>(){}.getType());
                if(list!=null) {
                    Message msg = new Message();
                    msg.obj = list;
                    handler.sendMessage(msg);
                }
            }
        });
    }
    public static List<Collection> getFromNet() throws IOException {
        OkHttpClient okHttpClient = new OkHttpClient();
        Request.Builder build = new Request.Builder();
        Request request = build.get().url("http://www.tangdesheng.cn:8080/collector/get").build();
        Call call = okHttpClient.newCall(request);

        Response response = call.execute();
        final String res = response.body().string();
        Gson gson = new Gson();
        final List<Collection> list;
        list = gson.fromJson(res.trim(),new TypeToken<List<Collection>>(){}.getType());
        return list;
    }

}
