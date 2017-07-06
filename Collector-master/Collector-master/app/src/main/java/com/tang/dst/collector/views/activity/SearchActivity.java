package com.tang.dst.collector.views.activity;

import android.app.Activity;
import android.os.Bundle;
import android.os.Handler;
import android.support.v4.widget.SwipeRefreshLayout;
import android.support.v7.widget.DefaultItemAnimator;
import android.support.v7.widget.LinearLayoutManager;
import android.support.v7.widget.RecyclerView;
import android.view.KeyEvent;
import android.view.View;
import android.widget.ImageButton;
import android.widget.LinearLayout;
import android.widget.TextView;

import com.tang.dst.collector.R;
import com.tang.dst.collector.database.SqlOperator;
import com.tang.dst.collector.entry.Collection;
import com.tang.dst.collector.tools.SearchListAdapter;
import com.tang.dst.collector.tools.TitleBarColorChange;
import com.tang.dst.collector.tools.Utils;
import com.tang.dst.collector.views.view.SearhEditText;

import java.util.List;

/**
 * Created by D.S.T on 16/12/13.
 */
public class SearchActivity extends Activity {
    private SearhEditText key;
    private ImageButton back;
    private RecyclerView result;
    public static boolean isMe = false;
    private SwipeRefreshLayout refreshLayout;
    private List<Collection> list;
    /*private Handler handler = new Handler(){
        @Override
        public void handleMessage(Message msg) {
            super.handleMessage(msg);
            list = (List<Collection>) msg.obj;
            initData(list);
        }
    };*/
    @Override
    public void onCreate(Bundle savedInstanceState) {
        new TitleBarColorChange().changeBarColor(this, R.color.title_bar_color);
        super.onCreate(savedInstanceState);
        /*try {
            new HttpUtils().getFromNet(handler);
        } catch (IOException e) {
            e.printStackTrace();
        }*/
        isMe = false;
        setContentView(R.layout.activity_search);
        MyView();
        DataRefresh();
        initData("");
    }

    private void MyView() {
        LinearLayout top = (LinearLayout) findViewById(R.id.top);
        top.setPadding(0, Utils.getStatusBarHeight(this), 0, 0);
        key = (SearhEditText) findViewById(R.id.keywords);
        result = (RecyclerView) findViewById(R.id.search_result);
        back = (ImageButton) findViewById(R.id.search_back);
        key.setOnEditorActionListener(new TextView.OnEditorActionListener() {
            @Override
            public boolean onEditorAction(TextView textView, int i, KeyEvent keyEvent) {
                if (key.getText().toString().isEmpty()) return false;
                initData(key.getText().toString());
                return true;
            }
        });
        key.setOnSearchClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                String keywords = key.getText().toString();
                initData(keywords);
            }
        });
        back.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                onBackPressed();
            }
        });
    }

    private void DataRefresh() {
        refreshLayout = (SwipeRefreshLayout) findViewById(R.id.search_refresh);
        //设置刷新时动画的颜色，可以设置4个
        refreshLayout.setColorSchemeResources(android.R.color.holo_blue_light, android.R.color.holo_red_light, android.R.color.holo_orange_light, android.R.color.holo_green_light);
        refreshLayout.setOnRefreshListener(new SwipeRefreshLayout.OnRefreshListener() {
            @Override
            public void onRefresh() {
                // TODO Auto-generated method stub
                new Handler().postDelayed(new Runnable() {
                    @Override
                    public void run() {
                        // TODO Auto-generated method stub
                        if (key.getText().toString().isEmpty()) {
                            refreshLayout.setRefreshing(false);
                            return;
                        }
                        result.removeAllViews();
                        initData(key.getText().toString());
                        refreshLayout.setRefreshing(false);
                    }
                }, 2000);
            }
        });
    }

    private void initData(String keyword) {
        if(keyword.isEmpty())return;
        SqlOperator sqlopp = new SqlOperator(this);
        result.setLayoutManager(new LinearLayoutManager(this));
        result.setItemAnimator(new DefaultItemAnimator());
        if (sqlopp.getSize() <= 0) return;
        List<Collection> col;
        col = sqlopp.query(keyword);
        isMe = true;
        SearchListAdapter adapter = new SearchListAdapter(this, col);
        result.setAdapter(adapter);
        sqlopp.close();
    }

    private void initData(List<Collection> list) {
        result.setLayoutManager(new LinearLayoutManager(this));
        result.setItemAnimator(new DefaultItemAnimator());
        if (list.size() <= 0) return;
        SearchListAdapter adapter = new SearchListAdapter(this, list);
        result.setAdapter(adapter);
    }

    @Override
    public void onBackPressed() {
        this.finish();
        super.onBackPressed();
    }

}
