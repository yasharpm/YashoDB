package com.yashoid.yashodb;

import com.yashoid.yashodb.exception.JSONException;
import com.yashoid.yashodb.exception.ValueTypeException;

import java.util.HashMap;
import java.util.Map;

public class YashON {

    public static YashON parse(String str) throws JSONException {
        return JSONParser.readObject(str.toCharArray(), 0).value;
    }

    private Map<String, Object> mMap;

    public YashON() {
        mMap = new HashMap<>();
    }

    public void put(String key, Object value) {
        validateKey(key);
        validateValue(value);

        mMap.put(key, value);
    }

    public Object get(String key) {
        return mMap.get(key);
    }

    public String getString(String key) throws ValueTypeException {
        return getString(get(key));
    }

    public Boolean getBoolean(String key) throws ValueTypeException {
        return getBoolean(get(key));
    }

    public Integer getInt(String key) throws ValueTypeException {
        return getInt(get(key));
    }

    public Long getLong(String key) throws ValueTypeException {
        return getLong(get(key));
    }

    public Float getFloat(String key) throws ValueTypeException {
        return getFloat(get(key));
    }

    public Double getDouble(String key) throws ValueTypeException {
        return getDouble(get(key));
    }

    public YashON getYashON(String key) throws ValueTypeException {
        return getYashON(get(key));
    }

    public YashAN getYashAN(String key) throws ValueTypeException {
        return getYashAN(get(key));
    }

    public boolean contains(String key) {
        return mMap.containsKey(key);
    }

    Map<String, Object> getAll() {
        return mMap;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append('{');

        boolean isFirst = true;

        for (Map.Entry<String, Object> entry: mMap.entrySet()) {
            if (!isFirst) {
                sb.append(',');
            }

            sb.append('"');
            sb.append(entry.getKey());
            sb.append('"');
            sb.append(':');

            Object value = entry.getValue();

            if (value == null) {
                sb.append("null");
            }
            else if (value instanceof YashON) {
                sb.append(value.toString());
            }
            else if (value instanceof YashAN) {
                sb.append(value.toString());
            }
            else {
                sb.append('"');
                appendString(sb, value.toString());
                sb.append('"');
            }

            isFirst = false;
        }

        sb.append('}');

        return sb.toString();
    }

    static void appendString(StringBuilder sb, String str) {
        sb.ensureCapacity(sb.length() + str.length());

        for (char c: str.toCharArray()) {
            if (c == '"') {
                sb.append('\\');
            }

            sb.append(c);
        }
    }

    static void validateKey(String key) {
        if (key == null) {
            throw new IllegalArgumentException("Key can not be null.");
        }

        if (!key.matches("[a-zA-Z0-9_]+")) {
            throw new IllegalArgumentException("Invalid key format. Must be '[a-zA-Z0-9_]+'.");
        }
    }

    static void validateValue(Object object) {
        boolean valid = object == null || object instanceof String || object instanceof Number ||
                object instanceof YashON || object instanceof YashAN;

        if (!valid) {
            throw new IllegalArgumentException(
                    "Invalid value type '" + object.getClass().getName() + "'. " +
                    "Must be either null, Number, String, YashON or YashAN.");
        }
    }

    static String getString(Object value) throws ValueTypeException {
        if (value == null) {
            return null;
        }

        if (value instanceof String) {
            return (String) value;
        }

        if (value instanceof Number) {
            return value.toString();
        }

        throw new ValueTypeException(null);
    }

    static Boolean getBoolean(Object value) throws ValueTypeException {
        if (value == null) {
            return null;
        }

        if (value instanceof Boolean) {
            return (Boolean) value;
        }

        if (value instanceof String) {
            try {
                return Boolean.parseBoolean((String) value);
            } catch (Throwable t) { }
        }

        throw new ValueTypeException(null);
    }

    static Number getNumber(Object value) throws ValueTypeException {
        if (value instanceof Number) {
            return (Number) value;
        }

        if (value instanceof String) {
            try {
                return ((Double) Double.parseDouble((String) value));
            } catch (Throwable t) { }
        }

        throw new ValueTypeException(null);
    }

    static Integer getInt(Object value) throws ValueTypeException {
        if (value == null) {
            return null;
        }

        return getNumber(value).intValue();
    }

    static Long getLong(Object value) throws ValueTypeException {
        if (value == null) {
            return null;
        }

        return getNumber(value).longValue();
    }

    static Float getFloat(Object value) throws ValueTypeException {
        if (value == null) {
            return null;
        }

        return getNumber(value).floatValue();
    }

    static Double getDouble(Object value) throws ValueTypeException {
        if (value == null) {
            return null;
        }

        return getNumber(value).doubleValue();
    }

    static YashON getYashON(Object value) throws ValueTypeException {
        if (value == null || value instanceof YashON) {
            return (YashON) value;
        }

        throw new ValueTypeException(null);
    }

    static YashAN getYashAN(Object value) throws ValueTypeException {
        if (value == null || value instanceof YashAN) {
            return (YashAN) value;
        }

        throw new ValueTypeException(null);
    }

}
