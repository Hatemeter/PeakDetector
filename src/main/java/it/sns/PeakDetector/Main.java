package it.sns.PeakDetector;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonReader;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.io.FileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
    public static void main(String[] args) {

        try {


            //// *******take care of this******* ///
            String mysqlUser = "";
            String mysqlPassword = "";
            String lang = "it"; // <- or en or fr
            //// ******************************* ///


            ConcurrentHashMap<String, Set<Long>> ht_idUnique = new ConcurrentHashMap<>();

            ConcurrentHashMap<String, TreeMap<String, AtomicInteger>> ht_daysCounter = new ConcurrentHashMap<>();
            String twitterFormat = "EEE MMM dd HH:mm:ss ZZZZZ yyyy";
            SimpleDateFormat sf = new SimpleDateFormat(twitterFormat, Locale.ENGLISH);
            SimpleDateFormat sfOut = new SimpleDateFormat("yyyy-MM-dd");

            for (int i = 0; i < args.length; i++) {
                try {
                    Gson gson = new GsonBuilder().create();

                    Path inputFile = Paths.get(args[i]);
                    String ht = inputFile.getFileName().toString().split("-")[2].replace(".json", "");
                    System.out.println(inputFile.getFileName());


                    ht_idUnique.putIfAbsent(ht, new HashSet<>());


                    JsonReader reader = new JsonReader(new FileReader(args[i]));
                    reader.beginArray();
                    while (reader.hasNext()) {
                        JsonObject tweet = gson.fromJson(reader, JsonObject.class);
                        long id = tweet.get("id").getAsLong();
                        if (tweet.get("lang").getAsString().equals(lang)) {
                            //System.out.println(tweet.get("created_at").toString());
                            //System.out.println(calGood.toString());
                            if (!ht_idUnique.get(ht).contains(id)) {
                                Date creation_date = sf.parse(tweet.get("created_at").getAsString());
                                Calendar cal = Calendar.getInstance();
                                cal.setTime(creation_date);
                                ht_daysCounter.putIfAbsent(ht, new TreeMap<>());
                                ht_daysCounter.get(ht).putIfAbsent(sfOut.format(cal.getTime()), new AtomicInteger(0));
                                ht_daysCounter.get(ht).get(sfOut.format(cal.getTime())).incrementAndGet();
                                ht_idUnique.get(ht).add(id);
                            } else {
                                //System.out.print("Duplicate for id:" + id + "...");
                            }
                        }
                    }
                    reader.endArray();
                    reader.close();


                } catch (Exception e) {
                    e.printStackTrace();
                }

            }

            ht_daysCounter.forEach((s, calendarAtomicIntegerTreeMap) -> {

                JsonObject htJson = new JsonObject();


                DescriptiveStatistics stats = new DescriptiveStatistics();

                JsonArray data = new JsonArray();
                JsonArray labels = new JsonArray();


                calendarAtomicIntegerTreeMap.forEach((calendar, atomicInteger) -> {
                    stats.addValue(Double.parseDouble(atomicInteger.toString()));
                    data.add(Double.parseDouble(atomicInteger.toString()));
                    labels.add(calendar);
                    // System.out.println("\t"+calendar +":"+ atomicInteger);
                });


                Double threshold = stats.getMean() + stats.getStandardDeviation();
                htJson.add("data", data);
                htJson.add("labels", labels);
                htJson.addProperty("threshold", threshold);

                Connection dbconn = null;
                try {
                    // Class.forName("org.gjt.mm.mysql.Driver");
                    Class.forName("com.mysql.cj.jdbc.Driver");
                    dbconn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/HateMeter?autoreconnect=true&allowMultiQueries=true&connectTimeout=0&socketTimeout=0&useUnicode=yes&characterEncoding=UTF-8&serverTimezone=UTC", mysqlUser, mysqlPassword);
                } catch (Exception e) {
                    e.printStackTrace();
                }


                try {
                    PreparedStatement pstmt = dbconn.prepareStatement("REPLACE INTO " + lang + "_alerts_trend VALUES (?,?)");
                    pstmt.setString(1, s);
                    pstmt.setString(2, htJson.toString());
                    pstmt.execute();
                    dbconn.close();

                } catch (Exception e) {
                    e.printStackTrace();
                }

            });



        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

