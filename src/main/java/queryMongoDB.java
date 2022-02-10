/**
* Name: Haifeng Zhang
* Date: 11/9/2021
* Course: CS-622
* Assignment 5
*/
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.MongoClient;
import java.io.*;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import static com.mongodb.client.model.Aggregates.*;

import java.text.ParseException;
import java.util.*;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.FindIterable;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import cn.hutool.json.JSONObject;
import cn.hutool.core.io.FileUtil;
import cn.hutool.json.JSON;
import cn.hutool.json.JSONConfig;
import cn.hutool.json.JSONUtil;
import java.text.SimpleDateFormat;
import java.text.DateFormat;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import com.mongodb.client.model.Filters;
import org.bson.conversions.Bson;


public class queryMongoDB {
    //read json and get value of the key "date"
    public static List<JSONObject> readJsonData(String filePath) throws IOException {
        Path path = Paths.get(filePath);
        File file = path.toFile();
        List<JSONObject> list = new ArrayList<>();

        List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        for (String line : lines) {
            JSONObject jsonObj = JSONUtil.parseObj(line);
            JSONObject data = jsonObj.getJSONObject("data");
            list.add(data);

        }


        return list;
    }


    public static void main(String[] args) throws IOException, ParseException {


        MongoClient client = new MongoClient("localhost", 27017);
        MongoDatabase database = client.getDatabase("mongotest");
        MongoCollection<Document> coll7 = database.getCollection("dataColl7");

        //bulkwrite the documents into the collection
        int count = 0;
        int batch =10;
        String path = "C:\\Users\\mzhf2\\Downloads\\CSS622H5Test\\query\\src\\main\\java\\test.json";
        List<InsertOneModel<Document>> docs =  new ArrayList<>();
        List<JSONObject> lines = readJsonData(path);
        for (JSONObject line:lines) {
            //write in one document each time
            docs.add(new InsertOneModel<>(Document.parse(line.toString())));
            count++;

            if (count == batch) {
                coll7.bulkWrite(docs, new BulkWriteOptions().ordered(false));
                docs.clear();
                count = 0;
            }
        }

        //sort the documents by launched_at time
        coll7.aggregate(Arrays.asList(sort(Sorts.descending("area")),
                        out("sorted"))).toCollection();
        MongoCollection<Document> sortedCollection = database.getCollection("sorted");




        System.out.println("=========================================================");
        //set up the keyword and time range
        Scanner sc= new Scanner(System.in);
        System.out.print("Enter the entry you want to search in the documents: ");
        String keyword= sc.nextLine();
        System.out.print("Enter the start date in the form MM/dd/yyyy HH:mm:ss: ");
        String fromDate= sc.nextLine();
        System.out.print("Enter the end date in the form MM/dd/yyyy HH:mm:ss: ");
        String toDate= sc.nextLine();



        long epochFrom = new java.text.SimpleDateFormat("MM/dd/yyyy HH:mm:ss").parse(fromDate).getTime();
        long epochTo = new java.text.SimpleDateFormat("MM/dd/yyyy HH:mm:ss").parse(toDate).getTime();

        Bson bson = Filters.and(Filters.gt("launched_at",epochFrom),
                Filters.lt("launched_at",epochTo),
                Filters.exists(keyword)
        );

        FindIterable<Document> findIterable2 =
                sortedCollection.find(bson);
        MongoCursor<Document> mongoCursor2 = findIterable2.iterator();
        while(mongoCursor2.hasNext()){
            System.out.println(mongoCursor2.next());

        }

        //store the output documents into a list of documents

        List<Document> results = new ArrayList<>();
        findIterable2.into(results);


        System.out.println("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=");


        if (results.isEmpty()) {
            System.out.println("No" + "There is " + keyword + "entry");
        } else {
            //get the date in the documents
            List<Integer> date = new ArrayList<>();
            for (Document result : results) {
                String json = result.toJson();
                JSONObject jsonObj = JSONUtil.parseObj(json);
                int launchedAtDate = jsonObj.getInt("launched_at");
                date.add(launchedAtDate);
            }
            System.out.print("Yes, there is an entry about "+ keyword + " on ");
            date.forEach(d ->{
                System.out.print(" "+ new Date(d) + ",");
            });
        }

    }
}