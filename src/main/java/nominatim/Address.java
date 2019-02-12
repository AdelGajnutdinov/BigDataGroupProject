package nominatim;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Address {
    private int lod = -1;
    private long osm_id = -1;
    private String osm_type = "";
    private String country_code = "";
    private String country = "";
    private String postcode = "";
    private String state = "";
    private String county = "";
    private String city = "";
    private String suburb = "";
    private String road = "";
    private String display_name = "";

    public Address(String json, int lod){
        try {
            JSONParser jsonParser = new JSONParser();
            JSONObject jObject = (JSONObject)jsonParser.parse(json);

            if(jObject.containsKey("error")){
                System.err.println(jObject.get("error"));
                return;
            }

            osm_id = (Long)jObject.get("osm_id");
            osm_type = (String)jObject.get("osm_type");
            display_name = (String) jObject.get("display_name");

            JSONObject addressObject = (JSONObject)jObject.get("address");
            if(addressObject.containsKey("country_code")){
                country_code = (String)addressObject.get("country_code");
            }
            if(addressObject.containsKey("country")){
                country = (String)addressObject.get("country");
            }
            if(addressObject.containsKey("postcode")){
                postcode = (String)addressObject.get("postcode");
            }
            if(addressObject.containsKey("state")){
                state = (String)addressObject.get("state");
            }
            if(addressObject.containsKey("county")){
                county = (String)addressObject.get("county");
            }
            if(addressObject.containsKey("city")){
                city = (String)addressObject.get("city");
            }
            if(addressObject.containsKey("suburb")){
                suburb = (String)addressObject.get("suburb");
            }
            if(addressObject.containsKey("road")){
                road = (String)addressObject.get("road");
            }

            this.lod = lod;
        }
        catch (ParseException e) {
            System.err.println("Can't parse JSON string");
            e.printStackTrace();
        }
    }

    public long getOsmId(){
        return osm_id;
    }

    public String getOsmType(){
        return osm_type;
    }

    public int getLod(){
        return lod;
    }

    public String getCountryCode(){
        return country_code;
    }

    public String getCountry(){
        return country;
    }

    public String getPostcode(){
        return postcode;
    }

    public String getState(){
        return state;
    }

    public String getCounty(){
        return county;
    }

    public String getCity(){
        return city;
    }

    public String getSuburb(){
        return suburb;
    }

    public String getRoad(){
        return road;
    }

    public String getDisplayName(){
        return display_name;
    }

    public String toString(){
        return display_name;
    }

}