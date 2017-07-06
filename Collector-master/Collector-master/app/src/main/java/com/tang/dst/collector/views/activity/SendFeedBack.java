package com.tang.dst.collector.views.activity;

import android.app.Activity;
import android.content.Intent;
import android.os.Bundle;
import android.os.Handler;
import android.os.Message;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.ImageButton;
import android.widget.Toast;

import com.tang.dst.collector.MainActivity;
import com.tang.dst.collector.R;
import com.tang.dst.collector.tools.HttpUtils;
import com.tang.dst.collector.tools.TitleBarColorChange;


public class SendFeedBack extends Activity implements View.OnClickListener {
    TitleBarColorChange tbcc = new TitleBarColorChange();
    private ImageButton fb_back;
    private EditText fb_message;
    private Button send_fb;
    private Intent intent = null;
    Handler handler = new Handler() {
        @Override
        public void handleMessage(Message msg) {
            super.handleMessage(msg);
            if (msg.what == 1)
                Toast.makeText(SendFeedBack.this, "反馈成功", Toast.LENGTH_SHORT).show();
            else
                Toast.makeText(SendFeedBack.this, "错误", Toast.LENGTH_SHORT).show();
        }
    };

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        tbcc.changeBarColor(this, R.color.title_bar_color);
        setContentView(R.layout.activity_feedback);
        Init();
    }

    private void Init() {
        fb_back = (ImageButton) findViewById(R.id.fb_back);
        fb_message = (EditText) findViewById(R.id.fb_et);
        send_fb = (Button) findViewById(R.id.send_fb);
        fb_back.setOnClickListener(this);
        send_fb.setOnClickListener(this);

    }

    @Override
    public void onClick(View view) {
        switch (view.getId()) {
            case R.id.fb_back:
                intent = new Intent(SendFeedBack.this, MainActivity.class);
                startActivity(intent);
                finish();
                tbcc.changeBarColor(this, R.color.title_bar_color);
                break;
            case R.id.send_fb:
                final String feedBackMsg = fb_message.getText().toString();
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            Message msg = new Message();
                            msg.what = HttpUtils.sendFeedBack(feedBackMsg);
                            handler.sendMessage(msg);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }).start();
                break;
            default:
                break;
        }
    }

    @Override
    public void onBackPressed() {
        super.onBackPressed();
        finish();
        tbcc.changeBarColor(this, R.color.title_bar_color);
    }
}
