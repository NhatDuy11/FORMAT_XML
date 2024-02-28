import com.eclipsesource.json.JsonArray;
import com.fasterxml.jackson.core.JsonParser;
import org.apache.kafka.common.protocol.types.Field;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class functional {
    public static void main(String[] args) {
        String str_json  ="{\"name\":\"John\", \"age\":31, \"city\":\"New York\"}";
        JSONParser jsonParser = new JSONParser();


        try {
            Object obj = jsonParser.parse(new FileReader("E:\\XML_FORMATSC\\test.json"));
          JSONArray array = (JSONArray) obj;
            System.out.println(array);
            for ( Object array_object : array) {
                JSONObject obj_get = (JSONObject) array_object;
                String name_obj =  (String) obj_get.get("name");
                List<String> cars_obj = (List<String>) obj_get.get("cars");
                    String joined = String.join("|",cars_obj);
                System.out.println("test :  "  + " "  + joined);

                System.out.println("name_obj" +name_obj);
          //      System.out.println("cars_obj" + cars_obj);


            }



                JSONObject object = (JSONObject) jsonParser.parse(str_json);
                String name = (String) object.get("name");
                Long age = (Long) object.get("age");
                String city =(String) object.get("city");
                System.out.println(city);
                System.out.println(name);
                System.out.println(age);






        }catch (Exception e) {
            e.printStackTrace();
        }




    }
}
