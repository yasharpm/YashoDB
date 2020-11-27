package com.yashoid.yashodb;

import java.io.IOException;

public class DBSelection {

    private YashoDB mDb;

    private boolean mOnlyOne;

    DBSelection(YashoDB db, boolean onlyOne) {
        mDb = db;
        mOnlyOne = onlyOne;
    }

    public Selection selection() {
        return new Selection();
    }

    class Selection { // TODO Remaining to implement: contains (without key not an object)

        private Selection mCloseTarget;

        private Filter mFilter;

        private boolean mAppended = false;

        private Selection() {

        }

        private Selection(Selection closeTarget) {
            mCloseTarget = closeTarget;
        }

        public Selection biggerThan(String key, Number value) {
            return setFilter(new Filter.ComparisonFilter(key, value, Filter.Operand.BT));
        }

        public Selection smallerThan(String key, Number value) {
            return setFilter(new Filter.ComparisonFilter(key, value, Filter.Operand.ST));
        }

        public Selection equalsTo(String key, Object value) {
            return setFilter(new Filter.ComparisonFilter(key, value, Filter.Operand.EQ));
        }

        public Selection notEqualsTo(String key, Object value) {
            return setFilter(new Filter.ComparisonFilter(key, value, Filter.Operand.NEQ));
        }

        public Selection biggerThanOrEqualTo(String key, Number value) {
            return setFilter(new Filter.ComparisonFilter(key, value, Filter.Operand.BQT));
        }

        public Selection smallerThanOrEqualTo(String key, Number value) {
            return setFilter(new Filter.ComparisonFilter(key, value, Filter.Operand.SQT));
        }

        public Selection isNull(String key) {
            return setFilter(new Filter.NullFilter(key));
        }

        public Selection isNotNull(String key) {
            return setFilter(new Filter.NotNullFilter(key));
        }

        private Selection setFilter(Filter filter) {
            mFilter = filter;

            if (mCloseTarget != null && mCloseTarget.mFilter instanceof Filter.WithinFilter) {
                return close();
            }

            return this;
        }

        public CountSelection count(String key) {
            return new CountSelection(this, key);
        }

        public Selection within(String key) {
            mFilter = new Filter.WithinFilter(key);

            return new Selection(this);
        }

        public Selection and() {
            mFilter = new Filter.AndFilter();

            return new Selection(this);
        }

        public Selection with() {
            mCloseTarget.append(mFilter);

            mAppended = true;

            return new Selection(mCloseTarget);
        }

        public Selection close() {
            mCloseTarget.append(mFilter);

            mAppended = true;

            if (mCloseTarget.mCloseTarget != null &&
                    mCloseTarget.mCloseTarget.mFilter instanceof Filter.WithinFilter) {
                return mCloseTarget.close();
            }

            return mCloseTarget;
        }

        private void append(Filter filter) {
            ((Filter.AppendableFilter) mFilter).append(filter);
        }

        public YashoDB commit() {
            if (mCloseTarget != null) {
                if (!mAppended) {
                    mCloseTarget.append(mFilter);
                }

                return mCloseTarget.commit();
            }

            Filter filter = mFilter;

            if (mOnlyOne) {
                filter = new Filter.OnlyOneFilter(mFilter);
            }

            final QueryResult queryResult = mDb.newQueryResult(filter);

            filter.setObserver(new Filter.Observer() {

                @Override
                public void accepted(long position) throws IOException {
                    queryResult.add(position);
                }

            });

            mDb.applyFilter(filter);

            return new YashoDB(mDb, queryResult);
        }

    }

    public static class CountSelection {

        private Selection selection;

        private String key;

        private CountSelection(Selection selection, String key) {
            this.selection = selection;

            this.key = key;
        }

        public Selection equalsTo(Object value) {
            selection.setFilter(new Filter.CountFilter(key, value, Filter.Operand.EQ));
            return selection;
        }

        public Selection notEqualsTo(Object value) {
            selection.setFilter(new Filter.CountFilter(key, value, Filter.Operand.NEQ));
            return selection;
        }

        public Selection biggerThan(Object value) {
            selection.setFilter(new Filter.CountFilter(key, value, Filter.Operand.BT));
            return selection;
        }

        public Selection smallerThan(Object value) {
            selection.setFilter(new Filter.CountFilter(key, value, Filter.Operand.ST));
            return selection;
        }

        public Selection biggerThanOrEqualTo(Object value) {
            selection.setFilter(new Filter.CountFilter(key, value, Filter.Operand.BQT));
            return selection;
        }

        public Selection smallerThanOrEqualTo(Object value) {
            selection.setFilter(new Filter.CountFilter(key, value, Filter.Operand.SQT));
            return selection;
        }

    }

}
