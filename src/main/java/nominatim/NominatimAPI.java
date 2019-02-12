package nominatim;

import javax.net.ssl.HttpsURLConnection;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

/**
 * Java library for reverse geocoding using Nominatim
 *
 * @version 0.2
 *
 */
public class NominatimAPI {
    private final String NominatimInstance = "https://nominatim.openstreetmap.org";

    private int zoomLevel = 18;

    public NominatimAPI(){}

    public NominatimAPI(int zoomLevel){
        if(zoomLevel < 0 || zoomLevel > 18){
            System.err.println("invalid zoom level, using default value");
            zoomLevel = 18;
        }

        this.zoomLevel = zoomLevel;
    }

    public Address getAddress(double lat, double lon){
        Address result = null;
        String urlString = NominatimInstance + "/reverse?format=json&addressdetails=1&lat=" + String.valueOf(lat) + "&lon=" + String.valueOf(lon) + "&zoom=" + zoomLevel + "&accept-language=de" ;
        System.out.println(urlString);
        try {
            result = new Address(getJSON(urlString), zoomLevel);
        } catch (IOException e) {
            System.err.println("Can't connect to server.");
            e.printStackTrace();
        }
        return result;
    }

    private String getJSON(String urlString) throws IOException{
        URL url = new URL(urlString);
        HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.addRequestProperty("User-Agent", "Mozilla/4.76");

        BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        String text;
        StringBuilder result = new StringBuilder();
        while ((text = in.readLine()) != null)
            result.append(text);

        in.close();
        return result.toString();
    }
}