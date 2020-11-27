package com.yashoid.yashodb;

import com.yashoid.yashodb.exception.JSONException;

import java.util.Locale;

public class JSONParser {

    static ReadValue<YashON> readObject(char[] chars, int index) throws JSONException {
        index = skipWhiteSpaces(chars, index);

        if (index == chars.length || chars[index] != '{') {
            throw new JSONException();
        }

        YashON yashON = new YashON();

        index++;

        index = skipWhiteSpaces(chars, index);

        while (index < chars.length && chars[index] != '}') {
            ReadValue<String> readString = readString(chars, index);

            String key = readString.value;

            index = readString.index;

            index = skipWhiteSpaces(chars, index);

            if (index == chars.length || chars[index] != ':') {
                throw new JSONException();
            }

            index = skipWhiteSpaces(chars, index + 1);

            ReadValue<?> readValue = readValue(chars, index);

            Object value = readValue.value;

            yashON.put(key, value);

            index = readValue.index;

            index = skipWhiteSpaces(chars, index);

            if (index == chars.length || (chars[index] != '}' && chars[index] != ',')) {
                throw new JSONException();
            }

            if (chars[index] == ',') {
                index++;
            }

            index = skipWhiteSpaces(chars, index);
        }

        if (index == chars.length) {
            throw new JSONException();
        }

        return new ReadValue<>(yashON, index + 1);
    }

    static ReadValue<YashAN> readArray(char[] chars, int index) throws JSONException {
        index = skipWhiteSpaces(chars, index);

        if (index == chars.length || chars[index] != '[') {
            throw new JSONException();
        }

        YashAN yashAN = new YashAN();

        index++;

        index = skipWhiteSpaces(chars, index);

        while (index < chars.length && chars[index] != ']') {
            ReadValue<?> readValue = readValue(chars, index);

            yashAN.put(readValue.value);

            index = readValue.index;

            index = skipWhiteSpaces(chars, index);

            if (index == chars.length || (chars[index] != ']' && chars[index] != ',')) {
                throw new JSONException();
            }

            if (chars[index] == ',') {
                index = skipWhiteSpaces(chars, index + 1);
            }
        }

        if (index == chars.length) {
            throw new JSONException();
        }

        return new ReadValue<>(yashAN, index + 1);
    }

    private static ReadValue<?> readValue(char[] chars, int index) throws JSONException {
        if (index == chars.length) {
            throw new JSONException();
        }

        char firstChar = chars[index];

        if (firstChar == '"') {
            return readString(chars, index);
        }
        else if (firstChar == '{') {
            return readObject(chars, index);
        }
        else if (firstChar == '[') {
            return readArray(chars, index);
        }
        else if (isNumeric(firstChar)) {
            return readNumber(chars, index);
        }
        else {
            return readBooleanOrNull(chars, index);
        }
    }

    private static ReadValue<Boolean> readBooleanOrNull(char[] chars, int index)
            throws JSONException {
        StringBuilder sb = new StringBuilder();

        while (index < chars.length && isAlphabetic(chars[index])) {
            sb.append(chars[index++]);
        }

        String rawValue = sb.toString().toLowerCase(Locale.US);

        if (rawValue.equals("true")) {
            return new ReadValue<>(true, index);
        }
        else if (rawValue.equals("false")) {
            return new ReadValue<>(false, index);
        }
        else if (rawValue.equals("null")) {
            return new ReadValue<>(null, index);
        }

        throw new JSONException();
    }

    private static ReadValue<Number> readNumber(char[] chars, int index) throws JSONException {
        StringBuilder sb = new StringBuilder();

        while (index < chars.length && isNumeric(chars[index])) {
            sb.append(chars[index++]);
        }

        try {
            Double doubleValue = Double.parseDouble(sb.toString());

            long longValue = doubleValue.longValue();

            Number value;

            if (doubleValue == longValue) {
                value = longValue;
            }
            else {
                value = doubleValue;
            }

            return new ReadValue<>(value, index);
        } catch (NumberFormatException e) {
            throw new JSONException("", e); // TODO
        }
    }

    private static ReadValue<String> readString(char[] chars, int index) throws JSONException {
        if (chars[index] != '"') {
            throw new JSONException();
        }

        StringBuilder sb = new StringBuilder();

        while (index + 1 < chars.length && chars[index + 1] != '"') {
            char c = chars[++index];

            if (c == '\\') {
                c = chars[++index];
            }

            sb.append(c);
        }

        if (index + 1 == chars.length) {
            throw new JSONException();
        }

        return new ReadValue<>(sb.toString(), index + 2);
    }

    private static int skipWhiteSpaces(char[] chars, int index) {
        while (index < chars.length && isWhiteSpace(chars[index])) {
            index++;
        }

        return index;
    }

    private static boolean isWhiteSpace(char c) {
        return c == ' ' || c == '\n' || c == '\t';
    }

    private static boolean isNumeric(char c) {
        return c == '-' || c == '.' || c == '+' || (c >= '0' && c <= '9');
    }

    private static boolean isAlphabetic(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
    }

    static class ReadValue<T> {

        T value;
        int index;

        ReadValue(T value, int index) {
            this.value = value;
            this.index = index;
        }

    }

}
