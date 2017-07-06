package com.tang.dst.collector.tools;

import android.app.Service;
import android.content.Intent;
import android.os.IBinder;
import android.support.annotation.Nullable;

import com.tang.dst.collector.views.view.Desktop;

/**
 * Created by D.S.T on 17/1/18.
 */
public class FloggleButton extends Service {
    @Nullable
    @Override
    public IBinder onBind(Intent intent) {
        return null;
    }

    private Desktop mDesk;

    @Override
    public void onCreate() {
        super.onCreate();
        init();
    }
    private void init(){
        mDesk = new Desktop(this);
        mDesk.show();
    }
}
