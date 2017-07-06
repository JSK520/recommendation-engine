package com.tang.dst.collector.views.view;

import android.content.Context;
import android.graphics.Rect;
import android.graphics.drawable.Drawable;
import android.text.Editable;
import android.text.TextWatcher;
import android.util.AttributeSet;
import android.view.MotionEvent;
import android.view.View;
import android.widget.EditText;

import com.tang.dst.collector.R;

/**
 * Created by D.S.T on 16/12/14.
 */
public class SearhEditText extends EditText {
    private final static String TAG = "SearchEditText";
    private Drawable imgInable;
    private Drawable imgAble;
    private Context mContext;
    private OnClickListener mSearchClickListener;
    public interface SearchClickListener{
        void onSearchClickListener(View view,String keywords);
    }
    public void setOnSearchClickListener(OnClickListener listener){
        this.mSearchClickListener = listener;
    }
    public SearhEditText(Context context) {
        super(context);
        mContext = context;
        init();
    }

    public SearhEditText(Context context, AttributeSet attrs) {
        super(context, attrs);
        mContext = context;
        init();
    }

    public SearhEditText(Context context, AttributeSet attrs, int defStyleAttr) {
        super(context, attrs, defStyleAttr);
        mContext = context;
        init();
    }

    private void init() {
        imgInable = mContext.getResources().getDrawable(android.R.drawable.ic_menu_search);
        imgAble = mContext.getResources().getDrawable(R.drawable.search_red);
        addTextChangedListener(new TextWatcher() {
            @Override
            public void onTextChanged(CharSequence s, int start, int before, int count) {}
            @Override
            public void beforeTextChanged(CharSequence s, int start, int count, int after) {}
            @Override
            public void afterTextChanged(Editable s) {
                setDrawable();
            }
        });
        setDrawable();
    }

    //设置搜索图片
    private void setDrawable() {
        if(length() < 1)
            setCompoundDrawablesWithIntrinsicBounds(null, null, imgInable, null);
        else
            setCompoundDrawablesWithIntrinsicBounds(null, null, imgAble, null);
    }

    // 处理搜索事件
    @Override
    public boolean onTouchEvent(MotionEvent event) {
        if (imgAble != null && event.getAction() == MotionEvent.ACTION_UP) {
            int eventX = (int) event.getRawX();
            int eventY = (int) event.getRawY();
            //Log.e(TAG, "X = " + eventX + "; Y = " + eventY);
            Rect rect = new Rect();
            getGlobalVisibleRect(rect);
            rect.left = rect.right - 80;
            if(rect.contains(eventX, eventY)) {
                setOnClickListener(mSearchClickListener);
            }
        }
        return super.onTouchEvent(event);
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
    }
}
