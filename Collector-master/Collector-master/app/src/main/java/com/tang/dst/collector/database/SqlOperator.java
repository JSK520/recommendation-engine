package com.tang.dst.collector.database;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;

import com.tang.dst.collector.entry.Collection;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by D.S.T on 16/12/1.
 */
public class SqlOperator {
    private static final String DATABASE_NAME = "favorite.db";
    private static final String TABLE_NAME = "myshare";
    private SQLiteHelper sqlHelper;
    private SQLiteDatabase db = null;

    public SqlOperator(Context context) {
        create(context);
    }

    public void create(Context context) {
        sqlHelper = new SQLiteHelper(context, DATABASE_NAME, null, 2);
        db = sqlHelper.getWritableDatabase();
    }

    public void close() {
        if (db != null) {
            db.close();
        }
        if (sqlHelper != null) {
            sqlHelper.close();
        }
    }

    public void insert(Collection col) {
        db = sqlHelper.getWritableDatabase();
        ContentValues values = new ContentValues();
        values.put("title", col.getTitle());
        values.put("content", col.getContent());
        values.put("time", col.getTime());
        values.put("isfavor", col.getIsfavor());
        db.insert(TABLE_NAME, null, values);
        values.clear();
        db.close();
    }

    public void delete(int id) {
        db = sqlHelper.getWritableDatabase();
        String sql = "DELETE FROM " + TABLE_NAME + " WHERE id='" + id + "'";
        db.execSQL(sql);
        db.close();
    }

    public List<Collection> query(String keyword) {
        db = sqlHelper.getWritableDatabase();
        String sql = "SELECT * FROM " + TABLE_NAME;
        Cursor cursor = db.rawQuery(sql, null);
        List<Collection> list = new ArrayList<Collection>();
        Collection col = null;
        while (cursor.moveToNext()) {
            String title = cursor.getString(1);
            String content = cursor.getString(2);
            if (title.indexOf(keyword) != -1 || content.indexOf(keyword) != -1) {
                col = new Collection(cursor.getInt(0), title, content, cursor.getString(3), cursor.getInt(4));
                list.add(col);
            }
        }
        cursor.close();
        db.close();
        return list;
    }

    public List<Collection> query() {
        db = sqlHelper.getWritableDatabase();
        String sql = "SELECT * FROM " + TABLE_NAME;
        Cursor cursor = db.rawQuery(sql, null);
        List<Collection> list = new ArrayList<Collection>();
        Collection col;
        while (cursor.moveToNext()) {
            col = new Collection(cursor.getInt(0), cursor.getString(1), cursor.getString(2), cursor.getString(3), cursor.getInt(4));
            list.add(col);
        }
        cursor.close();
        db.close();
        return list;
    }

    public List<Collection> queryLike() {
        db = sqlHelper.getWritableDatabase();
        String sql = "SELECT * FROM " + TABLE_NAME + " where isfavor = 1";
        Cursor cursor = db.rawQuery(sql, null);
        List<Collection> list = new ArrayList<Collection>();
        Collection col;
        while (cursor.moveToNext()) {
            col = new Collection(cursor.getInt(0), cursor.getString(1), cursor.getString(2), cursor.getString(3), cursor.getInt(4));
            list.add(col);
        }
        cursor.close();
        db.close();
        return list;
    }

    public void Like(int id) {
        db = sqlHelper.getWritableDatabase();
        String sql = "update " + TABLE_NAME + " set isfavor=1 " + "where id=" + "'" + id + "'";
        db.execSQL(sql);
        db.close();
    }

    public void disLike(int id) {
        db = sqlHelper.getWritableDatabase();
        String sql = "update " + TABLE_NAME + " set isfavor=0 " + "where id=" + "'" + id + "'";
        db.execSQL(sql);
        db.close();
    }

    public boolean isLoved(int id) {
        db = sqlHelper.getWritableDatabase();
        String sql = "SELECT isfavor FROM " + TABLE_NAME + " where id = '" + id + "'";
        Cursor cursor = db.rawQuery(sql, null);
        int isloved = -1;
        while (cursor.moveToNext()) {
            isloved = cursor.getInt(0);
        }
        if (isloved == 1)
            return true;
        cursor.close();
        db.close();
        return false;
    }

    public int getSize() {
        db = sqlHelper.getWritableDatabase();
        String sql = "SELECT * FROM " + TABLE_NAME;
        Cursor cursor = db.rawQuery(sql, null);
        int tle = 0;
        while (cursor.moveToNext()) {
            tle++;
        }
        cursor.close();
        db.close();
        return tle;
    }
}
