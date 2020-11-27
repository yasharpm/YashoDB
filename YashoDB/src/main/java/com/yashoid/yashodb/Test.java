package com.yashoid.yashodb;

import com.yashoid.yashodb.exception.JSONException;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

class Test {

    public static void main(String... args) {
        File file = new File("test.db");

        if (file.exists()) {
            file.delete();
        }

        YashoDB db = new YashoDB(file);

        db.open();

        YashON yashon = new YashON();
        yashon.put("name", "Yashar");

        YashAN grades = new YashAN();
        grades.put(12);
        grades.put(10);
        grades.put(19.5);

        YashON nested1 = new YashON();
        nested1.put("course", "maths");
        grades.put(nested1);

        yashon.put("grades", grades);

        YashON yashon2 = new YashON();
        yashon2.put("name", "Hasan");

        YashAN grades2 = new YashAN();
        grades2.put(13);
        grades2.put(10);

        YashON nested2 = new YashON();
        nested2.put("course", "chem");
        grades2.put(nested2);

        yashon2.put("grades", grades2);

        db.insertAll(Arrays.asList(yashon, yashon2));

        YashON yashON = db.get(1).asYashON();
        System.out.println(yashON.toString());

        YashoDB result = db.findAll().selection().equalsTo("name", "Yashar").commit();
        yashON = result.get(0).asYashON();
        System.out.println(yashON.toString());

        YashoDB gradesDb = db.findOne().selection().equalsTo("name", "Yashar").commit().get(0).get("grades");
        YashAN yashAN = gradesDb.asYashAN();
        System.out.println(yashAN.toString());

        yashON = db.findAll().selection().within("grades").equalsTo("course", "chem").commit().get(0).asYashON();
        System.out.println(yashON.toString());

        db.findOne().selection().equalsTo("name", "Hasan").commit().remove();
        yashAN = db.asYashAN();
        System.out.println(yashAN.toString());

        db.get(0).remove("grades");
        yashAN = db.asYashAN();
        System.out.println(yashAN.toString());

        db.get(0).set("grades", "Something new.");
        yashAN = db.asYashAN();
        System.out.println(yashAN.toString());

        db.insert(yashon2);
        db.set("grades", new YashAN());
        yashAN = db.asYashAN();
        System.out.println(yashAN.toString());

        try {
            File backupFile = new File("backup.db");
            db.writeToFile(backupFile);
            YashoDB backupDB = new YashoDB(backupFile);
            backupDB.open();
            System.out.println(backupDB.asYashAN());
            backupDB.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        db.close();

        try {
            System.out.println(YashON.parse("{   \"name\" : null }").toString());
            System.out.println(YashAN.parse("[\"na\\\"me\", null, 234]").toString());
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }

    public void sampleCode() {
        YashoDB db = new YashoDB(new File(""));

        db.open();

//        db.createIndex("age");
//        db.createIndex("substances.name");

        db = db.findAll()
                .selection()
                    .and().biggerThan("age", 18).with().smallerThan("age", 60).close()
                .commit();

        long recordCount = db.getCount();

        YashON yashon = new YashON();
        yashon.put("name", "Yashar");

        YashON yashon2 = db.get(0).asYashON();
        YashAN yashan = db.asYashAN();

        db = db.get("courses");

        // Remove specific member(s) from the list
        db.findOne().selection().equalsTo("id", "213").commit().remove();

        // Remove a field from a member(s)
//            db.findOne().selection().equals("id", "213").commit().remove("age"); // Members[].remove(age)

//        db.updateOne()
//                .selection()
//                    .equals("nationalCode", "0872978291")
//                .set("age", 32)
//                .set("courses", yashson)
//                .remove("bloodType")
//                .commit();

        db.findOne()
                .selection()
                    .equalsTo("id", 123863927)
                .commit()
                .get("courses");

        // Get all packages containing a substance.
        db.findAll()
                .selection()
                    .within("substances").equalsTo("name", "DatePicker")
                .commit();

        db.insert(yashon);

        db.insertAll(Arrays.asList(yashon));

        db.close();
    }

}
