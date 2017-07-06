package com.tang.dst.collector.tools;

/**
 * Created by D.S.T on 16/12/1.
 */

import android.app.Activity;
import android.content.Intent;
import android.os.Bundle;
import android.support.v7.widget.RecyclerView;
import android.support.v7.widget.RecyclerView.ViewHolder;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.LinearLayout;
import android.widget.TextView;

import com.tang.dst.collector.R;
import com.tang.dst.collector.views.activity.DetailContent;
import com.tang.dst.collector.database.SqlOperator;
import com.tang.dst.collector.entry.Collection;

import java.util.List;

public class SearchListAdapter extends RecyclerView.Adapter<SearchListAdapter.MyViewHolder> {

    private List<Collection> mCol;
    private Activity mContext;
    private LayoutInflater inflater;
    /*public interface OnItemClickListener{
        void onItemClick(View view,int position);
    }*/
    public SearchListAdapter(Activity context, List<Collection> collection) {
        this.mContext = context;
        this.mCol = collection;
        inflater = LayoutInflater.from(mContext);
    }
    @Override
    public int getItemCount() {
        return mCol.size();
    }
    //填充onCreateViewHolder方法返回的holder中的控件
    @Override
    public void onBindViewHolder(final MyViewHolder holder, final int position) {
        holder.title.setText(mCol.get(position).getTitle());
        holder.content.setText(mCol.get(position).getContent());
        holder.time.setText(mCol.get(position).getTime());
        /*if (mListener != null) {
            holder.itemView.setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View view) {
                    mListener.onItemClick(holder.itemView,position);
                }
            });
        }*/
    }
    @Override
    public MyViewHolder onCreateViewHolder(ViewGroup parent, int viewType) {
        View view = inflater.inflate(R.layout.search_item, parent, false);
        MyViewHolder holder = new MyViewHolder(view,mContext);
        return holder;
    }
    //private OnItemClickListener mListener;
    /*public void setOnItemClickListener(OnItemClickListener listener) {
        this.mListener = listener;
    }*/

    //ViewHolder
    class MyViewHolder extends ViewHolder implements View.OnClickListener{

        public LinearLayout page;
        public TextView title, content, time;
        public MyViewHolder(View view, Activity activity) {
            super(view);
            ViewInit(view);
        }
        private void ViewInit(View view) {
            page = (LinearLayout) view.findViewById(R.id.mypage);
            title = (TextView) view.findViewById(R.id.mytitle);
            content = (TextView) view.findViewById(R.id.mycontent);
            time = (TextView) view.findViewById(R.id.the_time);
            page.setOnClickListener(this);
        }
        @Override
        public void onClick(View view) {
            final SqlOperator sqlop = null;
            final int position = getPosition();
            switch (view.getId()) {
                case R.id.mypage:
                    Intent intent = new Intent(mContext, DetailContent.class);
                    Bundle bundle = new Bundle();
                    bundle.putInt("id", mCol.get(position).getId());
                    bundle.putString("title",mCol.get(position).getTitle());
                    bundle.putString("content",mCol.get(position).getContent());
                    bundle.putString("time", mCol.get(position).getTime());
                    bundle.putInt("isfavor", mCol.get(position).getIsfavor());
                    intent.putExtras(bundle);
                    mContext.startActivity(intent);
                    mContext.finish();
                    break;
                default:
                    break;
            }
        }
    }
}