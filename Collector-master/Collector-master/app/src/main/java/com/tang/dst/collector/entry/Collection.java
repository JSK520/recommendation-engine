package com.tang.dst.collector.entry;

/**
 * Created by D.S.T on 16/12/4.
 */
public class Collection {
    private int id;
    private String title,content,time;
    private int isfavor;

    public Collection(int id, String title, String content, String time, int isfavor) {
        this.id = id;
        this.title = title;
        this.content = content;
        this.time = time;
        this.isfavor = isfavor;
    }
    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getIsfavor() {

        return isfavor;
    }

    public void setIsfavor(int isfavor) {
        this.isfavor = isfavor;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "Collection{" +
                "id=" + id +
                ", title='" + title + '\'' +
                ", content='" + content + '\'' +
                ", time='" + time + '\'' +
                ", isfavor=" + isfavor +
                '}';
    }
}
