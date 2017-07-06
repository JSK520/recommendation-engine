package com.tang.dst.collector;

import android.app.AlertDialog;
import android.content.DialogInterface;
import android.content.Intent;
import android.os.Bundle;
import android.os.Handler;
import android.os.Message;
import android.support.design.widget.NavigationView;
import android.support.v4.view.GravityCompat;
import android.support.v4.widget.DrawerLayout;
import android.support.v4.widget.SwipeRefreshLayout;
import android.support.v7.app.ActionBarDrawerToggle;
import android.support.v7.app.AppCompatActivity;
import android.support.v7.widget.DefaultItemAnimator;
import android.support.v7.widget.LinearLayoutManager;
import android.support.v7.widget.RecyclerView;
import android.support.v7.widget.Toolbar;
import android.text.Html;
import android.text.method.LinkMovementMethod;
import android.view.Menu;
import android.view.MenuItem;
import android.webkit.WebView;
import android.widget.TextView;
import android.widget.Toast;

import com.tang.dst.collector.entry.Collection;
import com.tang.dst.collector.tools.FloggleButton;
import com.tang.dst.collector.tools.HttpUtils;
import com.tang.dst.collector.tools.RecyclerViewAdapter;
import com.tang.dst.collector.tools.ShareTool;
import com.tang.dst.collector.tools.TitleBarColorChange;
import com.tang.dst.collector.views.activity.CirclePage;
import com.tang.dst.collector.views.activity.SearchActivity;
import com.tang.dst.collector.views.activity.SendFeedBack;

import java.io.IOException;
import java.util.List;

public class MainActivity extends AppCompatActivity
        implements NavigationView.OnNavigationItemSelectedListener {
    TitleBarColorChange tbcc = new TitleBarColorChange();
    public static int isMe = 0;
    private Toolbar toolbar;
    private List<Collection> col;
    private SwipeRefreshLayout refreshLayout;
    private AlertDialog.Builder builder;
    private long exitTime = 0;
    private Intent intent = null;
    private RecyclerView dataList;
    RecyclerViewAdapter adapter;
    private Handler data_handler = new Handler(){
        @Override
        public void handleMessage(Message msg) {
            super.handleMessage(msg);
            col = (List<Collection>) msg.obj;
            initData(col);
        }
    };

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        tbcc.changeBarColor(this, R.color.title_bar_color);
        setContentView(R.layout.activity_main);
        try {
            new HttpUtils().getFromNet(data_handler);
        } catch (IOException e) {
            e.printStackTrace();
        }
        toolbar = (Toolbar) findViewById(R.id.toolbar);
        setSupportActionBar(toolbar);

        DrawerLayout drawer = (DrawerLayout) findViewById(R.id.drawer_layout);
        ActionBarDrawerToggle toggle = new ActionBarDrawerToggle(
                this, drawer, toolbar, R.string.navigation_drawer_open, R.string.navigation_drawer_close);
        drawer.setDrawerListener(toggle);
        toggle.syncState();
        NavigationView navigationView = (NavigationView) findViewById(R.id.nav_view);
        navigationView.setNavigationItemSelectedListener(this);
        DataRefresh();
        initData(col);
    }

    private void DataRefresh() {
        refreshLayout = (SwipeRefreshLayout) findViewById(R.id.refresh);
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
                        initData(col);
                        refreshLayout.setRefreshing(false);
                    }
                }, 2000);
            }
        });
    }
    //数据初始化
    private void initData(List<Collection> mCol) {
        if(mCol==null) return;
        dataList = (RecyclerView) findViewById(R.id.data_list);
        dataList.setLayoutManager(new LinearLayoutManager(this));
        dataList.setItemAnimator(new DefaultItemAnimator());
        /*SqlOperator sqlOperator = new SqlOperator(this);
        col = sqlOperator.query();*/
        adapter = new RecyclerViewAdapter(this, mCol);
        adapter.notifyDataSetChanged();
        dataList.setAdapter(adapter);
    }
    //返回按键重写
    @Override
    public void onBackPressed() {
        DrawerLayout drawer = (DrawerLayout) findViewById(R.id.drawer_layout);
        if (drawer.isDrawerOpen(GravityCompat.START)) {
            drawer.closeDrawer(GravityCompat.START);
            tbcc.changeBarColor(this, R.color.title_bar_color);
        } else if ((System.currentTimeMillis() - exitTime) > 2000) {
            Toast.makeText(this, "再按一次退出", Toast.LENGTH_SHORT).show();
            exitTime = System.currentTimeMillis();
        } else {
            this.finish();
        }
    }

    //创建ActionBar的菜单
    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        getMenuInflater().inflate(R.menu.main, menu);
        return super.onCreateOptionsMenu(menu);
    }

    //菜单点击事件
    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        int id = item.getItemId();
        if (id == R.id.menu_search) {
            intent = new Intent(MainActivity.this, SearchActivity.class);
            startActivity(intent);
        }
        if (id == R.id.menu_share) {
            try {
                ShareTool sample = new ShareTool();
                sample.getShareList("超级收藏夹", "我在用超级收藏夹,超赞的喔~~" + "软件下载链接: # http://www.tangdesheng.cn/download/com.super.favorite.apk #", MainActivity.this);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return true;
        }
        if (id == R.id.menu_update) {

        }
        if (id == R.id.feedback) {
            intent = new Intent(MainActivity.this, SendFeedBack.class);
            startActivity(intent);
        }

        return super.onOptionsItemSelected(item);
    }

    //DrawerLayout左滑菜单事件
    @SuppressWarnings("StatementWithEmptyBody")
    @Override
    public boolean onNavigationItemSelected(MenuItem item) {
        // Handle navigation view item clicks here.
        int id = item.getItemId();
        if (id == R.id.local) {
            intent = new Intent(MainActivity.this, CirclePage.class);
            startActivity(intent);
        } else if (id == R.id.favor) {
            initData(col);
        } else if (id == R.id.help) {
            builder = new AlertDialog.Builder(this);
            builder.setIcon(R.mipmap.help);
            builder.setTitle("帮助");
            TextView ab = new TextView(this);
            String html = "\t\t\t\t详情请进";
            html += "\t<a href='http://www.tangdesheng.cn/collector'>" + "超级收藏夹官网" + "</a>";
            ab.setText(Html.fromHtml(html));
            ab.setMovementMethod(LinkMovementMethod.getInstance());
            builder.setView(ab);
            builder.setPositiveButton("确定", new DialogInterface.OnClickListener() {
                @Override
                public void onClick(DialogInterface dialogInterface, int i) {
                    dialogInterface.dismiss();
                }
            });
            builder.show();
        } /*else if (id == R.id.left_setting) {
            intent = new Intent(MainActivity.this, SettingsActivity.class);
            startActivity(intent);
            finish();
        }*/ else if (id == R.id.about) {
            builder = new AlertDialog.Builder(this);
            builder.setIcon(R.mipmap.about);
            builder.setTitle("关于");
            WebView img = new WebView(this);
            img.loadUrl("http://www.tangdesheng.cn/collector/API/api.html");
            builder.setView(img);
            builder.setPositiveButton("确定", new DialogInterface.OnClickListener() {
                @Override
                public void onClick(DialogInterface dialogInterface, int i) {
                    dialogInterface.dismiss();
                }
            });
            builder.show();
        } else if(id == R.id.back){
            intent = new Intent(this, FloggleButton.class);
            startService(intent);
            finish();
        }

        DrawerLayout drawer = (DrawerLayout) findViewById(R.id.drawer_layout);
        drawer.closeDrawer(GravityCompat.START);
        return true;
    }

}
