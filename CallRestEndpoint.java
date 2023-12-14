import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class CallRestEndpoint {

	public static void main(String[] args) {
		try {
			String apiUrl = "https://api.example.com/data"; // Replace with your API URL

			URL url = new URL(apiUrl);

			HttpURLConnection connection = (HttpURLConnection) url.openConnection();

			connection.setRequestMethod("GET");

			int responseCode = connection.getResponseCode();
			System.out.println("Response Code: " + responseCode);

			BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
			String line;
			StringBuilder response = new StringBuilder();

			while ((line = reader.readLine()) != null) {
				response.append(line);
			}
			reader.close();

			// Print the response from the API
			System.out.println("Response Body: " + response.toString());

			connection.disconnect();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
