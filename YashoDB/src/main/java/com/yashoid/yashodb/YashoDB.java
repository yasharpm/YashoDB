package com.yashoid.yashodb;

import com.yashoid.yashodb.exception.DBAccessException;
import com.yashoid.yashodb.exception.DBFormatException;
import com.yashoid.yashodb.exception.DBUseException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.Map;

/**
 * Collection Format
 * C [8-bytes item count] {[1-byte DELETED/NOT_DELETED] [8-bytes member length] [member data]}... [1-byte HAS_TAIL/HAS_NOT_TAIL] [8-bytes tail offset]
 *
 * Object Format
 * O {[1-byte DELETED/NOT_DELETED] [8-bytes key-value length] [4-bytes key length] [key data] [member data]}... [1-byte HAS_TAIL/HAS_NOT_TAIL] [8-bytes tail offset]
 *
 * Value Format
 * V [RandomAccessFile Unicode String bytes]
 *
 * Null Format
 * N
 */
public class YashoDB {

    public static final char COLLECTION = 'C';
    public static final char OBJECT = 'O';
    public static final char VALUE = 'V';

    private static final char HAS_TAIL = 'T';
    private static final char HAS_NOT_TAIL = 't';
    private static final char DELETED = 'R';
    private static final char NOT_DELETED = 'r';
    private static final char NULL = 'N';

    private static final Object KEY_NOT_FOUND = new Object();

    private YashoDB mParent = null;

//    private List<WeakReference<QueryResult>> mQueryResults = null;

    private File mFile;

    private RandomAccessFile mFileAccess;

    private QueryResult mQueryResult = null;

    private char mType;
    private long mStartOffset;
    private long mLength;

    public YashoDB(File file) {
        mFile = file;

        mType = COLLECTION;
        mStartOffset = 1;

//        mQueryResults = new ArrayList<>();
    }

    private YashoDB(YashoDB parent, long startOffset, long length) throws IOException {
        mParent = parent;

        mFile = parent.mFile;
        mFileAccess = parent.mFileAccess;

        mFileAccess.seek(startOffset);

        mType = (char) mFileAccess.readByte();
        mStartOffset = startOffset + 1;
        mLength = length - 1;
    }

    YashoDB(YashoDB parent, QueryResult queryResult) {
        mParent = parent;

        mFile = parent.mFile;
        mFileAccess = parent.mFileAccess;

        mType = parent.mType;
        mStartOffset = parent.mStartOffset;
        mLength = parent.mLength;

        mQueryResult = queryResult;
    }

    public void open() {
        try {
            mFileAccess = new RandomAccessFile(mFile, "rws");

            if (mFileAccess.length() == 0) {
                mFileAccess.writeByte(COLLECTION);
                mFileAccess.writeLong(0);
                mFileAccess.writeByte(HAS_NOT_TAIL);
                mFileAccess.writeLong(0L);
                mFileAccess.seek(mStartOffset);
            }

            mLength = mFileAccess.length() - 1;
        } catch (IOException e) {
            throw new DBAccessException(e);
        }
    }

    public void close() {
        try {
            mFileAccess.close();
        } catch (IOException e) {
            throw new DBAccessException(e);
        }
    }

    public char getType() {
        return mType;
    }

    public void writeToFile(final File file) throws IOException {
        if (mType != COLLECTION) {
            throw new IllegalStateException("Only collections can be exported.");
        }

        final RandomAccessFile fileAccess = new RandomAccessFile(file, "rw");

        fileAccess.writeByte(COLLECTION);
        fileAccess.writeLong(getCount());

        iterate(new Processor() {

            @Override
            public boolean process(long index, long length) throws IOException {
                fileAccess.writeByte(NOT_DELETED);

                long lengthOffset = fileAccess.getFilePointer();
                fileAccess.writeLong(0);

                long valueLength = writeValueToFile(fileAccess, length);

                long eofOffset = fileAccess.getFilePointer();

                fileAccess.seek(lengthOffset);
                fileAccess.writeLong(valueLength);

                fileAccess.seek(eofOffset);

                return true;
            }

        });

        fileAccess.writeByte(HAS_NOT_TAIL);
        fileAccess.writeLong(0L);

        fileAccess.close();
    }

    private long writeValueToFile(RandomAccessFile fileAccess, long length) throws IOException {
        byte type = mFileAccess.readByte();

        switch (type) {
            case COLLECTION:
                return writeCollectionToFile(fileAccess);
            case OBJECT:
                return writeObjectToFile(fileAccess);
            case VALUE:
                return writeStringValueToFile(fileAccess, length);
            case NULL:
                fileAccess.writeByte(NULL);
                return 1L;
            default:
                throw new RuntimeException("State lost or bad file format.");
        }
    }

    private long writeCollectionToFile(RandomAccessFile fileAccess) throws IOException {
        long collectionLength = 0;

        fileAccess.writeByte(COLLECTION);
        collectionLength++;

        fileAccess.writeLong(mFileAccess.readLong());
        collectionLength += 8;

        byte c = mFileAccess.readByte();

        while (c != HAS_NOT_TAIL) {
            if (c == NOT_DELETED) {
                fileAccess.writeByte(NOT_DELETED);
                collectionLength++;

                long length = mFileAccess.readLong();
                long srcLengthOffset = mFileAccess.getFilePointer();

                long lengthOffset = fileAccess.getFilePointer();
                fileAccess.writeLong(0L);
                collectionLength += 8;

                long valueLength = writeValueToFile(fileAccess, length);
                collectionLength += valueLength;

                fileAccess.seek(lengthOffset);
                fileAccess.writeLong(valueLength);
                fileAccess.seek(fileAccess.getFilePointer() + valueLength);

                mFileAccess.seek(srcLengthOffset + length);
            }
            else if (c == DELETED) {
                long length = mFileAccess.readLong();
                long srcLengthOffset = mFileAccess.getFilePointer();

                mFileAccess.seek(srcLengthOffset + length);
            }
            else if (c == HAS_TAIL) {
                long offset = mFileAccess.getFilePointer();

                mFileAccess.seek(offset);
            }
            else {
                throw new RuntimeException("Lost state or bad file format");
            }

            c = mFileAccess.readByte();
        }

        fileAccess.writeByte(HAS_NOT_TAIL);
        collectionLength++;

        fileAccess.writeLong(0L);
        collectionLength += 8;

        return collectionLength;
    }

    private long writeObjectToFile(RandomAccessFile fileAccess) throws IOException {
        long objectLength = 0;

        fileAccess.writeByte(OBJECT);
        objectLength++;

        byte c = mFileAccess.readByte();

        while (c != HAS_NOT_TAIL) {
            if (c == NOT_DELETED) {
                fileAccess.writeByte(NOT_DELETED);
                objectLength++;

                long keyValueLength = mFileAccess.readLong();
                long keyValueStartOffset = mFileAccess.getFilePointer();

                int keyLength = mFileAccess.readInt();
                long valueLength = keyValueLength - 4 - keyLength;

                String key = readKey(keyLength);

                long keyValueLengthOffset = fileAccess.getFilePointer();
                fileAccess.writeLong(0L);
                objectLength += 8;

                fileAccess.writeInt(keyLength);
                objectLength += 4;

                for (char kc: key.toCharArray()) fileAccess.writeByte(kc);
                objectLength += keyLength;

                long actualValueLength = writeValueToFile(fileAccess, valueLength);
                objectLength += actualValueLength;

                long eofOffset = fileAccess.getFilePointer();

                fileAccess.seek(keyValueLengthOffset);
                fileAccess.writeLong(4 + keyLength + actualValueLength);
                fileAccess.seek(eofOffset);

                mFileAccess.seek(keyValueStartOffset + keyValueLength);
            }
            else if (c == DELETED) {
                long length = mFileAccess.readLong();

                mFileAccess.seek(mFileAccess.getFilePointer() + length);
            }
            else if (c == HAS_TAIL) {
                long offset = mFileAccess.readLong();

                mFileAccess.seek(offset);
            }
            else {
                throw new RuntimeException("Lost state or bad file format.");
            }

            c = mFileAccess.readByte();
        }

        fileAccess.writeByte(HAS_NOT_TAIL);
        objectLength++;

        fileAccess.writeLong(0L);
        objectLength += 8;

        return objectLength;
    }

    private long writeStringValueToFile(RandomAccessFile fileAccess, long length) throws IOException {
        fileAccess.writeByte(VALUE);

        byte[] bytes = new byte[(int) (length - 1)];

        mFileAccess.readFully(bytes);

        fileAccess.write(bytes);

        return length;
    }

    public long getCount() {
        if (mType != COLLECTION) {
            throw new DBUseException("Only collections have a count.");
        }

        try {
            if (mQueryResult != null) {
                return mQueryResult.getCount();
            }
            else {
                mFileAccess.seek(mStartOffset);

                return mFileAccess.readLong();
            }
        } catch (IOException e) {
            throw new DBAccessException(e);
        }
    }

    public DBSelection findAll() {
        return new DBSelection(this, false);
    }

    public DBSelection findOne() {
        return new DBSelection(this, true);
    }

    QueryResult newQueryResult(Filter filter) {
        QueryResult queryResult = new QueryResult(mFile, filter);

//        registerQueryResult(queryResult);

        return queryResult;
    }

    // TODO This would have been done to notify changes to query results.
//    private void registerQueryResult(QueryResult queryResult) {
//        if (mParent == null) {
//            mQueryResults.add(new WeakReference<>(queryResult));
//        }
//        else {
//            mParent.registerQueryResult(queryResult);
//        }
//    }

    void applyFilter(final Filter filter) {
        if (mType != COLLECTION) {
            throw new DBUseException("Filter can only be applied on a collection.");
        }

        iterate(new Processor() {

            @Override
            public boolean process(long index, long length) throws IOException {
                if (!filter.onNewMember()) {
                    return false;
                }

                long position = mFileAccess.getFilePointer() - 8 - 1;

                applyFilterOnMember(filter);

                filter.onMemberReviewed(position);

                return true;
            }

        });
    }

    private void applyFilterOnMember(Filter filter) throws IOException {
        // We are here at the beginning of the official value.
        long offset = mFileAccess.getFilePointer() ;

        String key = filter.consider(null, null);

        while (key != null) {
            Object value = getValueForKey(key);

            key = filter.consider(key, value);

            mFileAccess.seek(offset);
        }
    }

    private Object getValueForKey(String key) throws IOException {
        String[] keys = key.split("\\.");

        long valueLength = 0;

        for (String k: keys) {
            byte type = mFileAccess.readByte();

            if (type != OBJECT) {
                return -1;
            }

            valueLength = goToKey(k);

            if (valueLength == -1) {
                return KEY_NOT_FOUND;
            }
        }

        return readValue(valueLength, true);
    }

    private long goToKey(String key) throws IOException {
        byte c = mFileAccess.readByte();

        while (c != HAS_NOT_TAIL) {
            if (c == HAS_TAIL) {
                long offset = mFileAccess.readLong();

                mFileAccess.seek(offset);
            }
            else if (c == DELETED) {
                long length = mFileAccess.readLong();

                mFileAccess.seek(mFileAccess.getFilePointer() + length);
            }
            else if (c == NOT_DELETED) {
                long keyValueLength = mFileAccess.readLong();
                int keyLength = mFileAccess.readInt();

                String k = readKey(keyLength);

                long valueLength = keyValueLength - keyLength - 4;

                if (k.equals(key)) {
                    return valueLength;
                }
                else {
                    mFileAccess.seek(mFileAccess.getFilePointer() + valueLength);
                }
            }
            else {
                throw new IllegalStateException("State is lost or bad file format.");
            }

            c = mFileAccess.readByte();
        }

        return -1;
    }

    public YashoDB get(long index) {
        if (index < 0) {
            throw new IllegalArgumentException("Member index starts from 0.");
        }

        long count = getCount();

        if (index >= count) {
            throw new IllegalArgumentException("Requested index is out of range: " + index + "/" + count);
        }

        try {
            seekToPosition(index);

            long length = mFileAccess.readLong();

            return new YashoDB(this, mFileAccess.getFilePointer(), length);
        } catch (IOException e) {
            throw new DBAccessException(e);
        }
    }

    public YashoDB get(String key) {
        if (mType != OBJECT) {
            throw new DBUseException("DB type is not an object.");
        }

        try {
            mFileAccess.seek(mStartOffset);

            long length = goToKey(key);

            if (length == -1) {
                return null;
            }

            return new YashoDB(this, mFileAccess.getFilePointer(), length);
        } catch (IOException e) {
            throw new DBAccessException(e);
        }
    }

    public long remove() {
        if (mType == OBJECT) {
            return mParent.removeNestedObject(mStartOffset);
        }

        if (mType != COLLECTION) {
            throw new RuntimeException("Can only remove collections and objects.");
        }

        final QueryResult queryResult = new QueryResult(mFile);

        iterate(new Processor() {

            @Override
            public boolean process(long index, long length) throws IOException {
                long position = mFileAccess.getFilePointer() - 8 - 1;

                mFileAccess.seek(position);
                mFileAccess.writeByte(DELETED);

                queryResult.add(position);

                return true;
            }

        });

        try {
            decrementCount(queryResult.getCount());
        } catch (IOException e) {
            throw new DBAccessException(e);
        }

        // TODO Notify removed.

        return queryResult.getCount();
    }

    private long removeNestedObject(long startOffset) {
        try {
            if (mType == COLLECTION) {
                mFileAccess.seek(startOffset - 1 - 8 - 1);

                mFileAccess.writeByte(DELETED);

                if (mQueryResult == null) {
                    decrementCount(1);
                }

                // TODO Notify removed.

                return 1;
            }

            mFileAccess.seek(mStartOffset);

            byte c = mFileAccess.readByte();

            while (c != HAS_NOT_TAIL) {
                if (c == NOT_DELETED) {
                    long keyValueLength = mFileAccess.readLong();

                    if (mFileAccess.getFilePointer() + keyValueLength > startOffset) {
                        mFileAccess.seek(mFileAccess.getFilePointer() - 8 - 1);
                        mFileAccess.writeByte(DELETED);

                        // TODO Notify removed.

                        return 1;
                    }

                    mFileAccess.seek(mFileAccess.getFilePointer() + keyValueLength);
                }
                else if (c == DELETED) {
                    long keyValueLength = mFileAccess.readLong();
                    mFileAccess.seek(mFileAccess.getFilePointer() + keyValueLength);
                }
                else if (c == HAS_TAIL) {
                    long offset = mFileAccess.readLong();
                    mFileAccess.seek(offset);
                }
                else {
                    throw new IllegalStateException("State is lost or bad file format.");
                }

                c = mFileAccess.readByte();
            }

            return 0;
        } catch (IOException e) {
            throw new DBAccessException(e);
        }
    }

    public boolean remove(String key) {
        if (mType != OBJECT) {
            throw new DBUseException("Database type is not an object.");
        }

        try {
            mFileAccess.seek(mStartOffset);

            long length = goToKey(key);

            if (length == -1) {
                return false;
            }

            mFileAccess.seek(mFileAccess.getFilePointer() - key.length() - 4 - 8 - 1);
            mFileAccess.writeByte(DELETED);

            // TODO Notify changed.

            return true;
        } catch (IOException e) {
            throw new DBAccessException(e);
        }
    }

    public long set(final String key, final Object value) {
        if (mType == OBJECT) {
            return setObjectKeyValue(key, value, true);
        }

        final QueryResult queryResult = new QueryResult(mFile);

        iterate(new Processor() {

            @Override
            public boolean process(long index, long length) throws IOException {
                queryResult.add(mFileAccess.getFilePointer() - 8 - 1);

                byte type = mFileAccess.readByte();

                if (type == OBJECT) {
                    new YashoDB(
                            YashoDB.this,
                            mFileAccess.getFilePointer() - 1,
                            length
                    ).setObjectKeyValue(key, value, false);
                }

                return true;
            }

        });

        // TODO Notify changed.

        return queryResult.getCount();
    }

    private long setObjectKeyValue(String key, Object value, boolean notify) {
        try {
            mFileAccess.seek(mStartOffset);

            long length = goToKey(key);

            if (length != -1) {
                mFileAccess.seek(mFileAccess.getFilePointer() - key.length() - 4 - 8 - 1);
                mFileAccess.writeByte(DELETED);
            }

            seekToEnd();

            mFileAccess.seek(mFileAccess.getFilePointer() - 1);
            mFileAccess.writeByte(HAS_TAIL);

            long offset = mFileAccess.length();

            mFileAccess.writeLong(offset);
            mFileAccess.seek(offset);

            writeKeyValue(key, value);

            mFileAccess.writeByte(HAS_NOT_TAIL);
            mFileAccess.writeLong(0);

            if (notify) {
                // TODO Notify changed.
            }

            return 1;
        } catch (IOException e) {
            throw new DBAccessException(e);
        }
    }

    public YashON asYashON() {
        if (mType != OBJECT) {
            throw new DBUseException("DB type is not an object.");
        }

        try {
            mFileAccess.seek(mStartOffset);

            return readObject();
        } catch (IOException e) {
            throw new DBAccessException(e);
        }
    }

    public YashAN asYashAN() {
        if (mType != COLLECTION) {
            throw new DBUseException("DB type is not a collection.");
        }

        long count = getCount();

        if (count > Integer.MAX_VALUE) {
            throw new RuntimeException("Requested collection is too big.");
        }

        final YashAN yashAN = new YashAN((int) count);

        iterate(new Processor() {

            @Override
            public boolean process(long index, long length) throws IOException {
                yashAN.put(readValue(length, false));
                return true;
            }

        });

        return yashAN;
    }

    public void insertAll(List<YashON> yashonList) {
        if (mQueryResult != null) {
            throw new DBUseException("Can not insert in query result.");
        }

        if (mType != COLLECTION) {
            throw new RuntimeException("Database type is not a collection.");
        }

        try {
            seekToEnd();

            mFileAccess.seek(mFileAccess.getFilePointer() - 1);
            mFileAccess.writeByte(HAS_TAIL);

            long fileEnd = mFileAccess.length();

            mFileAccess.writeLong(fileEnd);

            mFileAccess.seek(fileEnd);

            int size = yashonList.size();

            for (YashON yashon: yashonList) {
                writeObject(yashon);
            }

            mFileAccess.writeByte(HAS_NOT_TAIL);
            mFileAccess.writeLong(0L);

            incrementCount(size);

            // TODO Notify inserted.
        } catch (IOException e) {
            throw new DBAccessException(e);
        }
    }

    public void insert(YashON yashon) {
        if (mQueryResult != null) {
            throw new DBUseException("Can not insert in query result.");
        }

        if (mType != COLLECTION) {
            throw new DBUseException("Database type is not a collection.");
        }

        try {
            seekToEnd();

            mFileAccess.seek(mFileAccess.getFilePointer() - 1);
            mFileAccess.writeByte(HAS_TAIL);

            long fileEnd = mFileAccess.length();

            mFileAccess.writeLong(fileEnd);

            mFileAccess.seek(fileEnd);

            writeObject(yashon);

            mFileAccess.writeByte(HAS_NOT_TAIL);
            mFileAccess.writeLong(0L);

            incrementCount(1);

            // TODO Notify inserted.
        } catch (IOException e) {
            throw new DBAccessException(e);
        }
    }

    private YashON readObject() throws IOException {
        YashON yashON = new YashON();

        byte c = mFileAccess.readByte();

        while (c != HAS_NOT_TAIL) {
            if (c == NOT_DELETED) {
                readKeyValue(yashON);
            }
            else if (c == DELETED) {
                long length = mFileAccess.readLong();

                mFileAccess.seek(mFileAccess.getFilePointer() + length);
            }
            else if (c == HAS_TAIL) {
                long offset = mFileAccess.readLong();

                mFileAccess.seek(offset);
            }
            else {
                throw new IllegalStateException("State is lost or bad file format.");
            }

            c = mFileAccess.readByte();
        }

        return yashON;
    }

    private void readKeyValue(YashON yashON) throws IOException {
        long keyValueLength = mFileAccess.readLong();
        int keyLength = mFileAccess.readInt();

        long valueLength = keyValueLength - keyLength - 4;

        String key = readKey(keyLength);

        long valuePosition = mFileAccess.getFilePointer();

        Object value = readValue(valueLength, false);

        yashON.put(key, value);

        mFileAccess.seek(valuePosition + valueLength);
    }

    private String readKey(int keyLength) throws IOException {
        byte[] keyBytes = new byte[keyLength];

        mFileAccess.readFully(keyBytes);

        char[] keyChars = new char[keyLength];

        for (int i = 0; i < keyLength; i++) {
            keyChars[i] = (char) keyBytes[i];
        }

        return new String(keyChars);
    }

    private Object readValue(long length, boolean asDataBase) throws IOException {
        byte type = mFileAccess.readByte();

        if (type == OBJECT) {
            if (asDataBase) {
                return new YashoDB(this, mFileAccess.getFilePointer() - 1, length);
            }

            return readObject();
        }
        else if (type == NULL) {
            return null;
        }
        else if (type == COLLECTION) {
            if (asDataBase) {
                return new YashoDB(this, mFileAccess.getFilePointer() - 1, length);
            }

            return readCollection();
        }
        else if (type == VALUE) {
            return readStringValue(length - 1);
        }
        else {
            throw new IllegalStateException("Expected a type character but encountered '" + type + "'.");
        }
    }

    private YashAN readCollection() throws IOException {
        long count = mFileAccess.readLong();

        if (count > Integer.MAX_VALUE) {
            throw new IllegalStateException("Collection size exceeds integer limits: " + count);
        }

        YashAN yashAN = new YashAN();

        byte c = mFileAccess.readByte();

        while (c != HAS_NOT_TAIL) {
            if (c == NOT_DELETED) {
                long objectLength = mFileAccess.readLong();
                long objectPosition = mFileAccess.getFilePointer();

                yashAN.put(readValue(objectLength, false));

                mFileAccess.seek(objectPosition + objectLength);
            }
            else if (c == DELETED) {
                long objectLength = mFileAccess.readLong();
                mFileAccess.seek(mFileAccess.getFilePointer() + objectLength);
            }
            else if (c == HAS_TAIL) {
                long offset = mFileAccess.readLong();

                mFileAccess.seek(offset);
            }
            else {
                throw new IllegalStateException("State is lost or bad file format.");
            }

            c = mFileAccess.readByte();
        }

        return yashAN;
    }

    private String readStringValue(long length) throws IOException {
        return mFileAccess.readUTF();
    }

    private void incrementCount(int size) throws IOException {
        mFileAccess.seek(mStartOffset);

        long count = mFileAccess.readLong() + size;

        mFileAccess.seek(mStartOffset);
        mFileAccess.writeLong(count);
    }

    private void decrementCount(long size) throws IOException {
        mFileAccess.seek(mStartOffset);

        long count = mFileAccess.readLong() - size;

        mFileAccess.seek(mStartOffset);
        mFileAccess.writeLong(count);
    }

    private void writeObject(YashON yashon) throws IOException {
        mFileAccess.writeByte(NOT_DELETED);

        long lengthOffset = mFileAccess.getFilePointer();
        mFileAccess.writeLong(0);

        long length = writeObjectValue(yashon);

        mFileAccess.seek(lengthOffset);
        mFileAccess.writeLong(length);

        mFileAccess.seek(lengthOffset + 8 + length);
    }

    private long writeObjectValue(YashON yashon) throws IOException {
        long length = 0;

        mFileAccess.writeByte(OBJECT);
        length++;

        Map<String, Object> map = yashon.getAll();

        for (Map.Entry<String, Object> entry: map.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            length += writeKeyValue(key, value);
        }

        mFileAccess.writeByte(HAS_NOT_TAIL);
        length++;

        mFileAccess.writeLong(0);
        length += 8;

        return length;
    }

    private long writeKeyValue(final String key, final Object value) throws IOException {
        mFileAccess.writeByte(NOT_DELETED);

        long length = 1;

        long keyValueLengthOffset = mFileAccess.getFilePointer();
        mFileAccess.writeLong(0);
        length += 8;
        long keyValueLength = 0;

        char[] keyChars = key.toCharArray();

        mFileAccess.writeInt(keyChars.length);
        keyValueLength += 4;

        for (char kc: keyChars) mFileAccess.writeByte(kc);
        keyValueLength += keyChars.length;

        keyValueLength += writeValue(value);

        long objectEndOffset = mFileAccess.getFilePointer();

        length += keyValueLength;

        mFileAccess.seek(keyValueLengthOffset);
        mFileAccess.writeLong(keyValueLength);

        mFileAccess.seek(objectEndOffset);

        return length;
    }

    private long writeArrayValue(YashAN yashan) throws IOException {
        long length = 0;

        mFileAccess.writeByte(COLLECTION);
        length++;

        List<Object> list = yashan.getAll();

        long size = list.size();

        mFileAccess.writeLong(size);
        length += 8;

        for (Object object: list) {
            mFileAccess.writeByte(NOT_DELETED);
            length++;

            long objectLengthOffset = mFileAccess.getFilePointer();
            mFileAccess.writeLong(0);
            length += 8;

            long objectLength = writeValue(object);

            length += objectLength;

            long arrayEndOffset = mFileAccess.getFilePointer();

            mFileAccess.seek(objectLengthOffset);
            mFileAccess.writeLong(objectLength);

            mFileAccess.seek(arrayEndOffset);
        }

        mFileAccess.writeByte(HAS_NOT_TAIL);
        length++;

        mFileAccess.writeLong(0);
        length += 8;

        return length;
    }

    private long writeValue(Object object) throws IOException {
        if (object instanceof YashON) {
            return writeObjectValue((YashON) object);
        }
        else if (object instanceof YashAN) {
            return writeArrayValue((YashAN) object);
        }
        else if (object == null) {
            mFileAccess.writeByte(NULL);
            return 1L;
        }
        else {
            String value = object.toString();

            mFileAccess.writeByte(VALUE);
            mFileAccess.writeUTF(value);

            return 1 + measureUTF8Length(value);
        }
    }

    /**
     * Seeks so that the file pointer is at the byte after the HAS_NOT_TAIL
     * @throws IOException
     */
    private void seekToEnd() throws IOException {
        mFileAccess.seek(mStartOffset);

        if (mType == COLLECTION) {
            mFileAccess.skipBytes(8);
        }

        byte c = mFileAccess.readByte();

        while (c != HAS_NOT_TAIL) {
            if (c == DELETED || c == NOT_DELETED) {
                long length = mFileAccess.readLong();

                mFileAccess.seek(mFileAccess.getFilePointer() + length);
            }
            else if (c == HAS_TAIL) {
                long offset = mFileAccess.readLong();

                mFileAccess.seek(offset);
            }
            else {
                throw new DBFormatException("Illegal character '" + c + "'.");
            }

            c = mFileAccess.readByte();
        }
    }

    /**
     * Returns with the file pointer at the byte after the deleted/not_deleted byte.
     * @param position
     * @throws IOException
     */
    private void seekToPosition(long position) throws IOException {
        if (mQueryResult != null) {
            long offset = mQueryResult.get(position);

            mFileAccess.seek(offset + 1);
        }
        else {
            seekToRawPosition(position);
        }
    }

    private void seekToRawPosition(long position) throws IOException {
        mFileAccess.seek(mStartOffset + 8);

        long index = -1;

        while (index <= position) {
            byte c = mFileAccess.readByte();

            if (c == HAS_NOT_TAIL) {
                throw new DBFormatException("Reached end of data base while still expecting more.");
            }

            if (c == HAS_TAIL) {
                long offset = mFileAccess.readLong();

                mFileAccess.seek(offset);
            }
            else if (c == DELETED) {
                long length = mFileAccess.readLong();

                mFileAccess.seek(mFileAccess.getFilePointer() + length);
            }
            else if (c == NOT_DELETED) {
                index++;

                if (index == position) {
                    return;
                }

                long length = mFileAccess.readLong();

                mFileAccess.seek(mFileAccess.getFilePointer() + length);
            }
            else {
                throw new DBFormatException("Illegal character '" + c + "'.");
            }
        }
    }

    private void iterate(Processor processor) {
        try {
            long index = 0;

            if (mQueryResult != null) {
                byte c;

                final long recordCount = mQueryResult.getRecordCount();

                for (long i = 0; i < recordCount; i++) {
                    mFileAccess.seek(mQueryResult.get(i));

                    c = mFileAccess.readByte();

                    if (c == NOT_DELETED) {
                        index++;

                        long length = mFileAccess.readLong();

                        if (!processor.process(index, length)) {
                            return;
                        }
                    }
                }
            }
            else {
                mFileAccess.seek(mStartOffset + 8);

                byte c = mFileAccess.readByte();

                while (c != HAS_NOT_TAIL) {
                    if (c == NOT_DELETED) {
                        index++;

                        long lengthPosition = mFileAccess.getFilePointer();
                        long length = mFileAccess.readLong();

                        if (!processor.process(index, length)) {
                            return;
                        }

                        mFileAccess.seek(lengthPosition + 8 + length);
                    }
                    else if (c == DELETED) {
                        long length = mFileAccess.readLong();

                        mFileAccess.seek(mFileAccess.getFilePointer() + length);
                    }
                    else if (c == HAS_TAIL) {
                        long offset = mFileAccess.readLong();

                        mFileAccess.seek(offset);
                    }
                    else {
                        throw new DBFormatException();
                    }

                    c = mFileAccess.readByte();
                }
            }
        } catch (IOException e) {
            throw new DBAccessException(e);
        }
    }

    private static int measureUTF8Length(String str) {
        int strLength = str.length();
        int encodedLength = 0;

        char c;

        for(int i = 0; i < strLength; ++i) {
            c = str.charAt(i);
            if (c >= 1 && c <= 127) {
                ++encodedLength;
            } else if (c > 2047) {
                encodedLength += 3;
            } else {
                encodedLength += 2;
            }
        }

        return encodedLength + 2;
    }

    private interface Processor {

        /**
         *
         * @param index
         * @param length
         * @return true if needs to continue.
         * @throws IOException
         */
        boolean process(long index, long length) throws IOException;

    }

}