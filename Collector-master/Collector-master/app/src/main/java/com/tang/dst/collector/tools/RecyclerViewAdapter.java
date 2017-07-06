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
import android.widget.CheckBox;
import android.widget.ImageButton;
import android.widget.LinearLayout;
import android.widget.TextView;

import com.tang.dst.collector.R;
import com.tang.dst.collector.database.SqlOperator;
import com.tang.dst.collector.entry.Collection;
import com.tang.dst.collector.views.activity.DetailContent;

import java.util.List;

public class RecyclerViewAdapter extends RecyclerView.Adapter<RecyclerViewAdapter.MyViewHolder> {

    private List<Collection> mCol;
    private Activity mContext;
    private LayoutInflater inflater;
    private RecyclerView mData;

    public interface OnItemClickListener {
        void onItemClick(View view, int position);
    }

    public RecyclerViewAdapter(Activity context, List<Collection> collection) {
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
        if (mListener != null) {
            holder.itemView.setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View view) {
                    mListener.onItemClick(holder.itemView, position);
                }
            });
        }
    }

    @Override
    public MyViewHolder onCreateViewHolder(ViewGroup parent, int viewType) {
        View view = inflater.inflate(R.layout.item_list, parent, false);
        MyViewHolder holder = new MyViewHolder(view, mContext);
        return holder;
    }

    private OnItemClickListener mListener;

    public void setOnItemClickListener(OnItemClickListener listener) {
        this.mListener = listener;
    }

    //ViewHolder
    class MyViewHolder extends ViewHolder implements View.OnClickListener,View.OnLongClickListener {

        public LinearLayout page;
        public TextView title, content, time;
        public ImageButton shareBtn;
        public CheckBox mymenu;

        public MyViewHolder(View view, Activity activity) {
            super(view);
            ViewInit(view);
        }

        private void ViewInit(View view) {
            page = (LinearLayout) view.findViewById(R.id.mypage);
            title = (TextView) view.findViewById(R.id.mytitle);
            content = (TextView) view.findViewById(R.id.mycontent);
            time = (TextView) view.findViewById(R.id.the_time);
            shareBtn = (ImageButton) view.findViewById(R.id.shareBtn);
            mymenu = (CheckBox) view.findViewById(R.id.mymenu);
            page.setOnClickListener(this);
            shareBtn.setOnClickListener(this);
            mymenu.setOnClickListener(this);
            page.setOnLongClickListener(this);
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
                    bundle.putString("title", mCol.get(position).getTitle());
                    bundle.putString("content", mCol.get(position).getContent());
                    bundle.putString("time", mCol.get(position).getTime());
                    bundle.putInt("isfavor", mCol.get(position).getIsfavor());
                    intent.putExtras(bundle);
                    mContext.startActivity(intent);
                    break;
                case R.id.shareBtn:
                    intent = new Intent(Intent.ACTION_SEND);
                    intent.setType("text/plain");
                    intent.putExtra(Intent.EXTRA_TEXT, "标题：" + mCol.get(position).getTitle() + "\n" + "内容：" + mCol.get(position).getContent());
                    mContext.startActivity(intent);
                    break;
                case R.id.mymenu:
                    if (mymenu.isChecked()) {
                        mymenu.setBackgroundResource(R.mipmap.arrow_down);
                        content.setMaxLines(content.getText().length());
                    } else {
                        mymenu.setBackgroundResource(R.mipmap.right);
                        content.setMaxLines(4);
                    }
                    page.invalidate();
                    break;
                default:
                    break;
            }
        }

        @Override
        public boolean onLongClick(View view) {
            switch(view.getId()){
                case R.id.mypage:
                    Utils.test(mContext,content);
                    break;
                default:
                    break;
            }
            return true;
        }
    }
}
/*PopupWindow 弹窗*/
/*View v = mContext.getLayoutInflater().inflate(R.layout.my_menu,null);
                    final PopupWindow pw = new PopupWindow(v, ViewGroup.LayoutParams.MATCH_PARENT,
                            ViewGroup.LayoutParams.WRAP_CONTENT,true);
                    pw.getContentView().measure(View.MeasureSpec.UNSPECIFIED, View.MeasureSpec.UNSPECIFIED);
                    pw.setTouchable(true);
                    pw.setOutsideTouchable(true);
                    pw.setBackgroundDrawable(new BitmapDrawable(mContext.getResources(),(Bitmap)null));
                    backgroundAlpha(0.5f);
                    pw.setOnDismissListener(new PopupWindow.OnDismissListener() {
                        @Override
                        public void onDismiss() {
                            backgroundAlpha(1.0f);
                        }
                    });
                    Button delete = (Button) v.findViewById(R.id.mydel);
                    delete.setOnClickListener(new View.OnClickListener() {
                        @Override
                        public void onClick(View view) {
                            new SqlOperator(mContext).delete(mCol.get(position).getId());
                            Data();
                            notifyDataSetChanged();
                            pw.dismiss();
                        }
                    });
                    pw.showAtLocation(mymenu,Gravity.CENTER,0,0);*/