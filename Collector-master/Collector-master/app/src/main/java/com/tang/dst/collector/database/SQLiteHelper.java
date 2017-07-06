package com.tang.dst.collector.database;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

/**
 * Created by D.S.T on 16/12/1.
 */
public class SQLiteHelper extends SQLiteOpenHelper {
    private static final String DATABASE_NAME = "favorite.db";
    private static final int DATABASE_VERSION = 1;
    private static final String TABLE_NAME = "myshare";
    private SQLiteDatabase db;

    public SQLiteHelper(Context context, String name, SQLiteDatabase.CursorFactory factory, int version) {
        super(context, name, factory, version);
        db = this.getWritableDatabase();
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        //String delete = "DROP TABLE IF EXISTS " + TABLE_NAME;
        //db.execSQL(delete);
        String sql = "CREATE TABLE " + TABLE_NAME + "("
                + "id integer primary key autoincrement,"
                + "title VARCHAR(16),"
                + "content VARCHAR(500),"
                + "time VARCHAR(40),"
                + "isfavor integer)";
        db.execSQL(sql);
        Log.e("sqlite", "数据库创建成功");
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int i, int i1) {
        /*String sql = "DROP TABLE IF EXISTS " + TABLE_NAME;
        db = this.getWritableDatabase();
        db.execSQL(sql);
        this.onCreate(db);
        db.close();
        Log.e("sqlite", "数据库更新成功");*/
    }

}
