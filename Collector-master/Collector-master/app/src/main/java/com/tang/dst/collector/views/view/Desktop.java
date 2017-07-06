package com.tang.dst.collector.views.view;

import android.content.Context;
import android.content.Intent;
import android.graphics.PixelFormat;
import android.graphics.Rect;
import android.util.AttributeSet;
import android.view.Gravity;
import android.view.LayoutInflater;
import android.view.MotionEvent;
import android.view.View;
import android.view.ViewGroup;
import android.view.WindowManager;
import android.widget.ImageButton;
import android.widget.LinearLayout;

import com.tang.dst.collector.MainActivity;
import com.tang.dst.collector.R;
import com.tang.dst.collector.tools.L;

/**
 * Created by D.S.T on 16/12/14.
 */
public class Desktop extends LinearLayout implements View.OnClickListener {
    private Context mContext;
    private ImageButton flogBtn;
    private View view;
    private WindowManager mWm;
    private WindowManager.LayoutParams mLp;
    float startX, startY;
    int top;
    float x, y;

    public Desktop(Context context) {
        super(context);
        init(context);
    }

    public Desktop(Context context, AttributeSet attrs) {
        super(context, attrs);
        init(context);
    }

    public Desktop(Context context, AttributeSet attrs, int defStyleAttr) {
        super(context, attrs, defStyleAttr);
        init(context);
    }

    public void create() {
        mWm = (WindowManager) mContext.getSystemService("window");
        mLp = new WindowManager.LayoutParams();
        mLp.type = WindowManager.LayoutParams.TYPE_SYSTEM_ALERT|WindowManager.LayoutParams.TYPE_PHONE;
        mLp.flags =WindowManager.LayoutParams.FLAG_NOT_TOUCH_MODAL | WindowManager.LayoutParams.FLAG_NOT_FOCUSABLE;
        mLp.format = PixelFormat.RGBA_8888;
        mLp.gravity = Gravity.TOP | Gravity.LEFT;
        mLp.x = 0;
        mLp.y = 0;
        mLp.width = WindowManager.LayoutParams.WRAP_CONTENT;
        mLp.height = WindowManager.LayoutParams.WRAP_CONTENT;
    }

    public void show() {
        mWm.addView(this, mLp);
    }

    public void onWindowFocusChanged(boolean hasFocus) {
        super.onWindowFocusChanged(hasFocus);
        Rect rect = new Rect();
        getWindowVisibleDisplayFrame(rect);
        top = rect.top;
    }

    /*@Override
    public boolean onTouchEvent(MotionEvent motionEvent) {
        x = motionEvent.getRawX();
        y = motionEvent.getRawY();
        L.e(x + " : " + y);
        switch (motionEvent.getAction()) {
            case MotionEvent.ACTION_DOWN:
                startX = motionEvent.getX();
                startY = motionEvent.getY();
                L.e(startX + " : " + startY);
                break;
            case MotionEvent.ACTION_MOVE:
                updateViewPosition();
                break;
            case MotionEvent.ACTION_UP:
                updateViewPosition();
                startX = startY = 0;
                break;
            default:
                break;
        }
        return true;
    }*/

    private void updateViewPosition() {
        mLp.x = (int) (x - startX);
        mLp.y = (int) (y - startY);
        L.e(mLp.x + " : " + mLp.y);
        mWm.updateViewLayout(view, mLp);
    }

    private void init(Context context) {
        this.mContext = context;
        this.mWm = (WindowManager) context.getSystemService("window");
        this.mLp = new WindowManager.LayoutParams(ViewGroup.LayoutParams.WRAP_CONTENT, ViewGroup.LayoutParams.WRAP_CONTENT);
        create();
        setOrientation(LinearLayout.VERTICAL);
        this.setLayoutParams(mLp);
        view = LayoutInflater.from(context).inflate(R.layout.floggle_btn, null);
        view.setOnTouchListener(new OnTouchListener() {
            @Override
            public boolean onTouch(View view, MotionEvent motionEvent) {
                x = motionEvent.getRawX();
                y = motionEvent.getRawY();
                L.e(x + " : " + y);
                switch (motionEvent.getAction()) {
                    case MotionEvent.ACTION_DOWN:
                        startX = motionEvent.getX();
                        startY = motionEvent.getY();
                        L.e(startX + " : " + startY);
                        break;
                    case MotionEvent.ACTION_MOVE:
                        updateViewPosition();
                        break;
                    case MotionEvent.ACTION_UP:
                        updateViewPosition();
                        startX = startY = 0;
                        break;
                    default:
                        break;
                }
                return true;
            }
        });
        this.addView(view);
        flogBtn = (ImageButton) view.findViewById(R.id.floggle_btn);
        flogBtn.setOnClickListener(this);
    }

    @Override
    public void onClick(View v) {
        switch (v.getId()) {
            case R.id.floggle_btn:
                Intent intent = new Intent(mContext, MainActivity.class);
                intent.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
                mContext.startActivity(intent);
                break;
            default:
                break;
        }
    }
}

/*
<?xml version="1.0" encoding="utf-8"?>
<shape xmlns:android="http://schemas.android.com/apk/res/android"
    android:shape="rectangle">
    <!-- 边角半径 -->
    <corners android:radius="50dp"/>
    <!-- 倾斜度 -->
    <gradient android:type="linear" />
    <!-- 背景padding值 -->
    <padding android:left="10dp" android:top="10dp" android:right="10dp" android:bottom="10dp"/>
    <!-- 背景宽高 -->
    <size android:width="50dp" android:height="50dp"/>
    <!-- 背景颜色 -->
    <solid android:color="@color/tran_black"
        />
    <!-- 边线 -->
    <stroke android:width="1px" android:color="#BEC1C6"/>
</shape>
 */