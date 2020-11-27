package com.yashoid.yashodb.sample;

import android.app.Activity;
import android.os.Bundle;
import android.util.Log;

import com.yashoid.yashodb.JsonException;
import com.yashoid.yashodb.JsonState;
import com.yashoid.yashodb.RandomAccessJson;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class MainActivity extends Activity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        File file = new File(getExternalCacheDir(), "test.db");

        try {
            file.createNewFile();

            FileOutputStream out = new FileOutputStream(file);

            JSONObject json = new JSONObject();
            json.put("name", "yashar");
            json.put("age", 29);

            String sson = json.toString();
            byte[] bytes = sson.getBytes("UTF-8");

            Log.d("AAA", sson.length() + ", " + bytes.length + "> " + sson);

            out.write(bytes);
            out.flush();
            out.close();

            RandomAccessJson raj = new RandomAccessJson(file);

            JsonState state = raj.getState();

            while (state != JsonState.DOCUMENT_END) {
                Log.d("AAA", "state = " + state);

                state = raj.getNextState();
            }

            Log.d("AAA", "state = " + state);


//            RandomAccessJson2 raj = new RandomAccessJson2(file);
//
//            raj.beginObject();
//
//            while (raj.hasNext()) {
//                String name = raj.nextName();
//
//                if ("name".equals(name)) {
//                    String s = raj.nextString();
//                    Log.d("AAA", "value: " + s);
//                }
//                else {
//                    raj.skipValue();
//                }
//            }
//
//            raj.endObject();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (JSONException e) {
            e.printStackTrace();
        } catch (JsonException e) {
            e.printStackTrace();
        }
    }

}
