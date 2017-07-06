package com.tang.dst.collector.views.activity;

import android.app.Activity;
import android.os.Bundle;
import android.os.Handler;
import android.os.Message;
import android.support.v7.widget.DefaultItemAnimator;
import android.support.v7.widget.LinearLayoutManager;
import android.support.v7.widget.RecyclerView;
import android.view.View;
import android.widget.ImageButton;
import android.widget.LinearLayout;

import com.tang.dst.collector.R;
import com.tang.dst.collector.entry.Collection;
import com.tang.dst.collector.tools.HttpUtils;
import com.tang.dst.collector.tools.RecyclerViewAdapter;
import com.tang.dst.collector.tools.TitleBarColorChange;
import com.tang.dst.collector.tools.Utils;

import java.io.IOException;
import java.util.List;

/**
 * Created by D.S.T on 16/12/8.
 */
public class CirclePage extends Activity {
    private ImageButton back;
    private List<Collection> sharedList;
    private Handler data_handler = new Handler(){
        @Override
        public void handleMessage(Message msg) {
            super.handleMessage(msg);
            sharedList = (List<Collection>) msg.obj;
            RecyclerInit(sharedList);
        }
    };

    @Override
    public void onCreate(Bundle savedInstanceState) {
        new TitleBarColorChange().changeBarColor(this, R.color.title_bar_color);
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_friends);
        try {
            new HttpUtils().getFromNet(data_handler);
        } catch (IOException e) {
            e.printStackTrace();
        }
        RecyclerInit(sharedList);
        FriView();
        LinearLayout top = (LinearLayout) findViewById(R.id.top);
        top.setPadding(0, Utils.getStatusBarHeight(this), 0, 0);
    }

    private void FriView() {
        back = (ImageButton) findViewById(R.id.fri_back);
        back.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                onBackPressed();
            }
        });
    }

    private void RecyclerInit(List<Collection> list) {
        if(list == null) return;
        RecyclerView mov = (RecyclerView) findViewById(R.id.mov);
        mov.setLayoutManager(new LinearLayoutManager(this));
        mov.setItemAnimator(new DefaultItemAnimator());
        /*mov.addItemDecoration(new DividerItemDecoration(
                this, DividerItemDecoration.HORIZONTAL));*/
        RecyclerViewAdapter adapter = new RecyclerViewAdapter(this,list);
        adapter.notifyDataSetChanged();
        mov.setAdapter(adapter);
    }

    @Override
    public void onBackPressed() {
        super.onBackPressed();
        finish();
    }
}
