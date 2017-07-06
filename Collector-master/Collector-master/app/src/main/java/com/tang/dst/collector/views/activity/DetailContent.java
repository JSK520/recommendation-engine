package com.tang.dst.collector.views.activity;

import android.app.Activity;
import android.content.Intent;
import android.os.Bundle;
import android.text.method.ScrollingMovementMethod;
import android.view.View;
import android.widget.ImageButton;
import android.widget.LinearLayout;
import android.widget.TextView;

import com.tang.dst.collector.R;
import com.tang.dst.collector.entry.Collection;
import com.tang.dst.collector.tools.TitleBarColorChange;
import com.tang.dst.collector.tools.Utils;

/**
 * Created by D.S.T on 16/12/4.
 */
public class DetailContent extends Activity implements View.OnClickListener{
    TitleBarColorChange tbcc = new TitleBarColorChange();
    LinearLayout top;
    TextView content_title,content_time,con_content;
    ImageButton con_back,con_share;
    Intent intent;
    Collection collections;
    @Override
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        tbcc.changeBarColor(this, R.color.title_bar_color);
        setContentView(R.layout.content_page);
        MyViewInit();
        BtnOnclick();
        top.setPadding(0, Utils.getStatusBarHeight(this),0,0);
    }

    private void MyViewInit(){
        top = (LinearLayout) findViewById(R.id.top);
        content_title = (TextView) findViewById(R.id.content_title);
        content_time = (TextView) findViewById(R.id.content_time);
        con_content = (TextView) findViewById(R.id.myessay);
        con_back = (ImageButton) findViewById(R.id.page_back);
        con_share  = (ImageButton) findViewById(R.id.content_share);
        //获取intent传来的数据
        Intent intent = getIntent();
        Bundle bundle = intent.getExtras();
        int id,isfavor;
        String title,content,time;
        id = bundle.getInt("id");
        title = bundle.getString("title");
        content = bundle.getString("content");
        time  = bundle.getString("time");
        isfavor = bundle.getInt("isfavor");
        collections = new Collection(id,title,content,time,isfavor);
        content_title.setText(collections.getTitle());
        content_time.setText(collections.getTime());
        con_content.setText("\u3000\u3000"+collections.getContent());
        con_content.setTextIsSelectable(true);
        con_content.setCursorVisible(true);
        con_content.setOnLongClickListener(new View.OnLongClickListener() {
            @Override
            public boolean onLongClick(View arg0) {
                Utils.test(DetailContent.this,con_content);
                return true;
            }
        });
        con_content.setMovementMethod(ScrollingMovementMethod.getInstance());
    }

    private void BtnOnclick(){
        con_back.setOnClickListener(this);
        con_share.setOnClickListener(this);
    }
    @Override
    public void onBackPressed() {
        this.finish();
        super.onBackPressed();
    }
    @Override
    public void onClick(View view) {
        switch (view.getId()){
            case R.id.page_back:
                onBackPressed();
                break;
            /*case R.id.content_like:
                sqlopp = new SqlOperator(this);
                if (sqlopp.isLoved(collections.getId())) {
                    sqlopp.disLike(collections.getId());
                    con_like.setImageResource(R.mipmap.heart_e);
                    try {
                        HttpUtils.saveSharedToNet(DetailContent.this,collections);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else {
                    sqlopp.Like(collections.getId());
                    con_like.setImageResource(R.mipmap.heart_f);
                }
                sqlopp.close();
                break;*/
            case R.id.content_share:
                /*try {
                    ShareTool sample = new ShareTool();
                    sample.getShareList(collections.getTitle(),collections.getContent(), DetailContent.this);
                } catch (Exception e) {
                    e.printStackTrace();
                }*/
                intent = new Intent(Intent.ACTION_SEND);
                intent.setType("text/plain");
                intent.putExtra(Intent.EXTRA_TEXT,"标题："+collections.getTitle()+"\n"+"内容："+collections.getContent());
                startActivity(intent);
                break;
            default:
                break;
        }
    }
}
