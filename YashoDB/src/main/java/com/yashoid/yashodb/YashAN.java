package com.yashoid.yashodb;

import com.yashoid.yashodb.exception.JSONException;
import com.yashoid.yashodb.exception.ValueTypeException;

import java.util.ArrayList;
import java.util.List;

public class YashAN {

    public static YashAN parse(String str) throws JSONException {
        return JSONParser.readArray(str.toCharArray(), 0).value;
    }

    private List<Object> mList;

    public YashAN() {
        mList = new ArrayList<>();
    }

    public YashAN(int size) {
        mList = new ArrayList<>(size);
    }

    List<Object> getAll() {
        return mList;
    }

    public int getCount() {
        return mList.size();
    }

    public void put(Object object) {
        YashON.validateValue(object);

        mList.add(object);
    }

    public Object get(int index) {
        return mList.get(index);
    }

    public String getString(int index) throws ValueTypeException {
        return YashON.getString(get(index));
    }

    public Boolean getBoolean(int index) throws ValueTypeException {
        return YashON.getBoolean(get(index));
    }

    public Integer getInt(int index) throws ValueTypeException {
        return YashON.getInt(get(index));
    }

    public Long getLong(int index) throws ValueTypeException {
        return YashON.getLong(get(index));
    }

    public Float getFloat(int index) throws ValueTypeException {
        return YashON.getFloat(get(index));
    }

    public Double getDouble(int index) throws ValueTypeException {
        return YashON.getDouble(get(index));
    }

    public YashON getYashON(int index) throws ValueTypeException {
        return YashON.getYashON(get(index));
    }

    public YashAN getYashAN(int index) throws ValueTypeException {
        return YashON.getYashAN(get(index));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append('[');

        boolean isFirst = true;

        for (Object obj: mList) {
            if (!isFirst) {
                sb.append(',');
            }

            if (obj == null) {
                sb.append("null");
            }
            else if (obj instanceof  YashON) {
                sb.append(obj.toString());
            }
            else if (obj instanceof YashAN) {
                sb.append(obj.toString());
            }
            else {
                sb.append('"');
                YashON.appendString(sb, obj.toString());
                sb.append('"');
            }

            isFirst = false;
        }

        sb.append(']');

        return sb.toString();
    }

}
