package com.yashoid.yashodb;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

class QueryResult {

    private Filter mFilter = null;

    private List<Long> mSelectedMemberPositions = new LinkedList<>();

    private File mQueryFile;

    private RandomAccessFile mQueryFileAccess = null;

    private long mCount = 0;
    private long mRecordCount = 0;

    QueryResult(File dbFile) {
        mQueryFile = new File(dbFile.getParentFile(), "query-" + UUID.randomUUID().toString());
    }

    QueryResult(File dbFile, Filter filter) {
        this(dbFile);

        mFilter = filter;
    }

    long get(long index) throws IOException {
        if (mQueryFileAccess == null) {
            return mSelectedMemberPositions.get((int) index);
        }

        mQueryFileAccess.seek(index * 8);

        long position = mQueryFileAccess.readLong();

        mQueryFileAccess.seek(mQueryFileAccess.length());

        return position;
    }

    void add(long position) throws IOException {
        mCount++;
        mRecordCount++;

        if (mQueryFileAccess == null) {
            mSelectedMemberPositions.add(position);

            if (mSelectedMemberPositions.size() == Integer.MAX_VALUE) {
                mQueryFileAccess = new RandomAccessFile(mQueryFile, "rw");

                for (long memberPosition: mSelectedMemberPositions) {
                    mQueryFileAccess.writeLong(memberPosition);
                }

                mSelectedMemberPositions = null;
            }
        }
        else {
            mQueryFileAccess.writeLong(position);
        }
    }

    long getCount() {
        return mCount;
    }

    long getRecordCount() {
        return mRecordCount;
    }

    void onRemoved(long position) {
        // TODO
    }

    void onModified(long position) {
        // TODO ?
    }

}
