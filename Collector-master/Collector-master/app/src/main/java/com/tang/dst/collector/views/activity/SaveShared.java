package com.tang.dst.collector.views.activity;

import android.annotation.TargetApi;
import android.app.Activity;
import android.content.Intent;
import android.os.Build;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.CheckBox;
import android.widget.EditText;
import android.widget.LinearLayout;
import android.widget.Toast;

import com.tang.dst.collector.MainActivity;
import com.tang.dst.collector.R;
import com.tang.dst.collector.database.SqlOperator;
import com.tang.dst.collector.entry.Collection;
import com.tang.dst.collector.tools.HttpUtils;
import com.tang.dst.collector.tools.TitleBarColorChange;
import com.tang.dst.collector.tools.Utils;

import java.io.IOException;

/**
 * Created by D.S.T on 16/11/30.
 */
public class SaveShared extends Activity implements View.OnClickListener {
    TitleBarColorChange tbcc = new TitleBarColorChange();
    private LinearLayout top;
    private Button cancel, save;
    private EditText title, content;
    private String res;
    private CheckBox cb;

    @Override
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        tbcc.changeBarColor(this, R.color.title_bar_color);
        setContentView(R.layout.activity_saveshared);
        initView();
        initEvent();
        top.setPadding(0, Utils.getStatusBarHeight(this), 0, 0);
    }

    /**
     * 控件初始化
     */
    private void initView() {
        cancel = (Button) findViewById(R.id.cancel);
        save = (Button) findViewById(R.id.save);
        content = (EditText) findViewById(R.id.share_content);
        title = (EditText) findViewById(R.id.share_title);
        top = (LinearLayout) findViewById(R.id.top);
        cb = (CheckBox) findViewById(R.id.share_box);
    }

    /**
     * 事件初始化
     */
    private void initEvent() {
        cancel.setOnClickListener(this);
        save.setOnClickListener(this);
        getIntentOfMine();
    }

    public void getIntentOfMine() {
        Intent intent = getIntent();
        if (intent == null)
            return;
        Bundle extras = intent.getExtras();

        if (extras == null)
            return;

        switch (intent.getType()) {
            case "text/plain"://分享的内容类型，如果png图片：image/png
                MainActivity.isMe = 0;
                content.setText(extras.get(Intent.EXTRA_TEXT) + "");
                break;
            default:
                break;
        }
    }

    /**
     * 响应事件
     */
    @Override
    public void onClick(View view) {
        switch (view.getId()) {
            case R.id.cancel:
                onBackPressed();
                break;
            case R.id.save:
                String titleText = title.getText().toString();
                String contentText = content.getText().toString();
                String time = Utils.getTime();
                Collection col = new Collection(0,titleText,contentText,time,0);
                if (titleText.equals("")) {
                    Toast.makeText(this, "请输入标题", Toast.LENGTH_SHORT).show();
                } else if (contentText.equals("")) {
                    Toast.makeText(this, "请选择收藏内容", Toast.LENGTH_SHORT).show();
                } else {
                    if(cb.isChecked()){
                        col.setIsfavor(1);
                        try {
                            HttpUtils.saveSharedToNet(SaveShared.this,col);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }else {
                        col.setIsfavor(0);
                        saveSharedToLocal(col);
                    }
                    Intent intent = new Intent(SaveShared.this, MainActivity.class);
                    startActivity(intent);
                    this.finish();
                }
                break;
            default:
                break;
        }
    }

    @TargetApi(Build.VERSION_CODES.N)
    private void saveSharedToLocal(Collection col) {
        SqlOperator opp = new SqlOperator(this);
        opp.insert(col);
    }

    @Override
    public void onBackPressed() {
        if (MainActivity.isMe == 1) {
            Intent intent = new Intent(SaveShared.this, MainActivity.class);
            startActivity(intent);
            this.finish();
        } else {
            this.finish();
        }
        super.onBackPressed();
    }

}
