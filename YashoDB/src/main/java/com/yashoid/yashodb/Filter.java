package com.yashoid.yashodb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

interface Filter {

    enum Operand { ST, SQT, EQ, NEQ, BQT, BT }

    /**
     *
     * @return true if you want this member and all the remaining members to be read.
     */
    boolean onNewMember();

    /**
     *
     * @param key   The key that has been read.
     * @param value It's value.
     * @return The name of the next key if you still need one.
     */
    String consider(String key, Object value) throws IOException;

    /**
     * All fields of the member have been reviewed and we are done with this member.
     * @param position The position of the processed member in the database.
     * @return true if this member is accepted.
     */
    boolean onMemberReviewed(long position) throws IOException;

    void setObserver(Observer observer);

    interface Observer {

        void accepted(long position) throws IOException;

    }

    class ComparisonFilter implements Filter {

        private String mKey;
        private Object mValue;
        private Operand mOperand;

        private boolean mValueIsNumber;
        private double mValueAsDouble;

        private Observer mObserver = null;

        private boolean mMatches;

        ComparisonFilter(String key, Object value, Operand operand) {
            mKey = key;
            mValue = value;
            mOperand = operand;

            try {
                mValueAsDouble = Double.parseDouble(value.toString());
                mValueIsNumber = true;
            } catch (Throwable t) {
                mValueIsNumber = false;
            }
        }

        @Override
        public void setObserver(Observer observer) {
            mObserver = observer;
        }

        @Override
        public boolean onNewMember() {
            mMatches = false;

            return true;
        }

        @Override
        public String consider(String key, Object value) {
            if (key == null) {
                return mKey;
            }

            if (value == null) {
                mMatches = false;
                return null;
            }

            if (mValueIsNumber) {
                try {
                    double valueAsDouble = Double.parseDouble(value.toString());

                    switch (mOperand) {
                        case EQ:
                            mMatches = mValueAsDouble == valueAsDouble;
                            break;
                        case NEQ:
                            mMatches = mValueAsDouble != valueAsDouble;
                            break;
                        case ST:
                            mMatches = valueAsDouble < mValueAsDouble;
                            break;
                        case SQT:
                            mMatches = valueAsDouble <= mValueAsDouble;
                            break;
                        case BT:
                            mMatches = valueAsDouble > mValueAsDouble;
                            break;
                        case BQT:
                            mMatches = valueAsDouble >= mValueAsDouble;
                    }

                    return null;
                } catch (NumberFormatException e) { }
            }

            switch (mOperand) {
                case EQ:
                    mMatches = mValue.equals(value);
                    break;
                case NEQ:
                    mMatches = !mValue.equals(value);
                    break;
            }

            return null;
        }

        @Override
        public boolean onMemberReviewed(long position) throws IOException {
            if (mMatches) {
                if (mObserver != null) {
                    mObserver.accepted(position);
                }

                return true;
            }

            return false;
        }

    }

    class NullFilter implements Filter {

        private String mKey;

        private Observer mObserver = null;

        private boolean mMatches;

        NullFilter(String key) {
            mKey = key;
        }

        @Override
        public void setObserver(Observer observer) {
            mObserver = observer;
        }

        @Override
        public boolean onNewMember() {
            mMatches = false;

            return true;
        }

        @Override
        public String consider(String key, Object value) {
            if (key == null) {
                return mKey;
            }

            mMatches = value == null;

            return null;
        }

        @Override
        public boolean onMemberReviewed(long position) throws IOException {
            if (mMatches) {
                if (mObserver != null) {
                    mObserver.accepted(position);
                }

                return true;
            }

            return false;
        }

    }

    class NotNullFilter implements Filter {

        private String mKey;

        private Observer mObserver = null;

        private boolean mMatches;

        NotNullFilter(String key) {
            mKey = key;
        }

        @Override
        public void setObserver(Observer observer) {
            mObserver = observer;
        }

        @Override
        public boolean onNewMember() {
            mMatches = false;

            return true;
        }

        @Override
        public String consider(String key, Object value) {
            if (key == null) {
                return mKey;
            }

            mMatches = value != null;

            return null;
        }

        @Override
        public boolean onMemberReviewed(long position) throws IOException {
            if (mMatches) {
                if (mObserver != null) {
                    mObserver.accepted(position);
                }

                return true;
            }

            return false;
        }

    }

    class CountFilter implements Filter {

        private String mKey;
        private long mValue;
        private Operand mOperand;

        private Observer mObserver = null;

        private boolean mMatches;

        CountFilter(String key, Object value, Operand operand) {
            mKey = key;
            mValue = Long.parseLong(value.toString());
            mOperand = operand;
        }

        @Override
        public void setObserver(Observer observer) {
            mObserver = observer;
        }

        @Override
        public boolean onNewMember() {
            mMatches = false;

            return true;
        }

        @Override
        public String consider(String key, Object value) throws IOException {
            if (key == null) {
                return mKey;
            }

            if (!(value instanceof YashoDB)) {
                return null;
            }

            YashoDB db = (YashoDB) value;

            if (db.getType() != YashoDB.COLLECTION) {
                return null;
            }

            long count = db.getCount();

            switch (mOperand) {
                case EQ:
                    mMatches = count == mValue;
                    break;
                case NEQ:
                    mMatches = count != mValue;
                    break;
                case BT:
                    mMatches = count > mValue;
                    break;
                case BQT:
                    mMatches = count >= mValue;
                    break;
                case ST:
                    mMatches = count < mValue;
                    break;
                case SQT:
                    mMatches = count <= mValue;
                    break;
            }

            return null;
        }

        @Override
        public boolean onMemberReviewed(long position) throws IOException {
            if (mMatches) {
                if (mObserver != null) {
                    mObserver.accepted(position);
                }

                return true;
            }

            return false;
        }

    }

    interface AppendableFilter extends Filter {

        void append(Filter filter);

    }

    class OnlyOneFilter implements Filter {

        private Filter mFilter;

        private boolean mNeedMore = true;

        private Observer mObserver = null;

        OnlyOneFilter(Filter filter) {
            mFilter = filter;
        }

        @Override
        public void setObserver(Observer observer) {
            mObserver = observer;
        }

        @Override
        public boolean onNewMember() {
            return mNeedMore && mFilter.onNewMember();
        }

        @Override
        public String consider(String key, Object value) throws IOException {
            return mFilter.consider(key, value);
        }

        @Override
        public boolean onMemberReviewed(long position) throws IOException {
            boolean accepted = mFilter.onMemberReviewed(position);

            if (accepted) {
                mNeedMore = false;

                mObserver.accepted(position);
            }

            return accepted;
        }

    }

    class WithinFilter implements AppendableFilter, Observer {

        private String mKey;

        private Filter mFilter;

        private boolean mMatches;

        private Observer mObserver = null;

        WithinFilter(String key) {
            mKey = key;
        }

        @Override
        public void setObserver(Observer observer) {
            mObserver = observer;
        }

        @Override
        public void append(Filter filter) {
            mFilter = filter;
        }

        @Override
        public boolean onNewMember() {
            mMatches = false;

            return true;
        }

        @Override
        public String consider(String key, Object value) throws IOException {
            if (key == null) {
                return mKey;
            }

            if (!(value instanceof YashoDB)) {
                return null;
            }

            YashoDB db = (YashoDB) value;

            if (db.getType() != YashoDB.COLLECTION) {
                return null;
            }

            Filter filter = new OnlyOneFilter(mFilter);

            filter.setObserver(this);

            db.applyFilter(filter);

            return null;
        }

        @Override
        public void accepted(long position) throws IOException {
            mMatches = true;
        }

        @Override
        public boolean onMemberReviewed(long position) throws IOException {
            if (mMatches) {
                if (mObserver != null) {
                    mObserver.accepted(position);
                }

                return true;
            }

            return false;
        }

    }

    class AndFilter implements AppendableFilter {

        private List<Filter> mFilters = new ArrayList<>();

        private Observer mObserver = null;

        private int mIndex;
        private boolean mMatches;

        @Override
        public void append(Filter filter) {
            mFilters.add(filter);
        }

        @Override
        public void setObserver(Observer observer) {
            mObserver = observer;
        }

        @Override
        public boolean onNewMember() {
            mMatches = true;
            mIndex = -1;

            return true;
        }

        @Override
        public String consider(String key, Object value) {
            if (mIndex == -1) {
//                return mFilters.get(++mIndex).
            }

            mIndex++;


            return null;
//            return (mIndex == mFilters.size() - 1) ? ;
        }

        @Override
        public boolean onMemberReviewed(long position) {
            return false;
        }
    }

    class OrFilter implements AppendableFilter {

        private List<Filter> mFilters = new ArrayList<>();

        private Observer mObserver = null;

        @Override
        public void setObserver(Observer observer) {
            mObserver = observer;
        }

        @Override
        public void append(Filter filter) {
            mFilters.add(filter);
        }

        @Override
        public boolean onNewMember() {
            return false;
        }

        @Override
        public String consider(String key, Object value) {
            return null;
        }

        @Override
        public boolean onMemberReviewed(long position) {
            return false;
        }

    }

}
