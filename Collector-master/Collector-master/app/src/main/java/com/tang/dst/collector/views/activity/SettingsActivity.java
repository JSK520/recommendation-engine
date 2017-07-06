package com.tang.dst.collector.views.activity;

import android.app.Activity;
import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.ImageButton;
import android.widget.TableRow;
import android.widget.Toast;

import com.tang.dst.collector.MainActivity;
import com.tang.dst.collector.R;
import com.tang.dst.collector.tools.TitleBarColorChange;


/**
 * Created by D.S.T on 16/8/27.
 */
public class SettingsActivity extends Activity implements View.OnClickListener {
    public Intent intent;
    TitleBarColorChange tbcc = new TitleBarColorChange();
    private ImageButton backHome;
    private TableRow myfavor;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        tbcc.changeBarColor(this, R.color.title_bar_color);
        setContentView(R.layout.activity_settings);
        ViewInit();
        EventInit();
    }

    private void ViewInit() {
        backHome = (ImageButton) findViewById(R.id.set_return);
        myfavor = (TableRow) findViewById(R.id.my_favor);
    }

    private void EventInit() {
        backHome.setOnClickListener(this);
        myfavor.setOnClickListener(this);
    }

    @Override
    public void onBackPressed() {
        super.onBackPressed();
        finish();
        tbcc.changeBarColor(this, R.color.title_bar_color);
    }

    @Override
    public void onClick(View view) {
        switch (view.getId()) {
            case R.id.set_return:
                intent = new Intent(SettingsActivity.this, MainActivity.class);
                startActivity(intent);
                finish();
                tbcc.changeBarColor(this, R.color.title_bar_color);
                break;
            case R.id.my_favor:
                Toast.makeText(this, "我的收藏", Toast.LENGTH_SHORT).show();
                break;
            default:
                break;
        }
    }
}
