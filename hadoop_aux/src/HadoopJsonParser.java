import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

public class HadoopJsonParser {

	public static int nAttrs = 0;
	public static int nextFileNumber = 0;
	public static int blockSize;
	
	public static String destDirectory;

	//<attrName, printwriter> 
	public static HashMap<String, BufferedWriter> currentOutputFiles;
	public static HashMap<String, Integer> sizeOfCurrentFiles;
	public static HashMap<String, String> attr2Class = new HashMap<String, String>();
	public static Set<String> nameOfAttrs;
	private static int tweetNr; 

	public static void main(String[] args) throws IOException, JsonSyntaxException, ClassNotFoundException {
		String srcDirectory = args[0];
		destDirectory = args[1];
		blockSize = Integer.parseInt(args[2]);

		initializeAttr2ClassMap();
		nameOfAttrs = attr2Class.keySet();
		newOutputFiles();

		File folder = new File(srcDirectory);
		File[] listOfFiles = folder.listFiles();
		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile()) {
				String fileName = listOfFiles[i].getAbsolutePath();
				if(fileName.contains(".gz")) {
					System.out.println("Parsing file " + fileName);

					FileInputStream fileStream = new FileInputStream(fileName);
					GZIPInputStream gzipStream = new GZIPInputStream(fileStream);
					InputStreamReader decoder = new InputStreamReader(gzipStream, "UTF-8");
					BufferedReader reader = new BufferedReader(decoder);

					parseFile(reader);
				}
			}
		}
		closeSetOfFiles();
	}

	private static void parseFile(BufferedReader reader) throws IOException, JsonSyntaxException, ClassNotFoundException {
		String tweetStr;
		tweetNr = 1;
		while ((tweetStr = reader.readLine()) != null) {

			JsonParser parser = new JsonParser();
			Gson gson = new Gson();

			JsonObject obj = parser.parse(tweetStr).getAsJsonObject();
			writeTweet(gson, obj);

			tweetNr++;
		}
		reader.close();
	}

	private static void writeTweet(Gson gson, JsonObject obj) throws JsonSyntaxException, IOException, ClassNotFoundException {
		checkSizes();
		for (String attr : attr2Class.keySet()) {
			String className = attr2Class.get(attr);
			BufferedWriter writer = currentOutputFiles.get(attr);
			try {
				Object element = gson.fromJson(obj.get(attr), Class.forName(className));
				String value = "";
				if (element == null) {
					value = "null";
				}
				else {
					value = element.toString();
					value = value.replace("\n", "\\n");
				}
				writer.append(value);	
				writer.newLine();
				updateSize(attr, value.getBytes("UTF-8").length);
			} catch(Exception e) {
				e.printStackTrace();
				System.out.println("attr " + attr);
				System.out.println("className " + className);
				System.out.println("tweet nr " + tweetNr);
			}
		}
	}

	private static void checkSizes() throws IOException {
		for(Integer size : sizeOfCurrentFiles.values()) {
			if (size.intValue() > blockSize) {
				closeSetOfFiles();
				newOutputFiles();
				break;
			}
		}
	}

	private static void closeSetOfFiles() throws IOException {
		for (BufferedWriter writer : currentOutputFiles.values()) {
			writer.close();
		}
		sizeOfCurrentFiles = new HashMap<String, Integer>();
	}

	private static void updateSize(String attr, int length) {
		int currentSize = sizeOfCurrentFiles.get(attr).intValue();
		sizeOfCurrentFiles.put(attr, new Integer(currentSize + length));
	}

	private static void newOutputFiles() throws IOException {
		currentOutputFiles = new HashMap<String, BufferedWriter>();
		sizeOfCurrentFiles = new HashMap<String, Integer>();
		int i = 0;
		for(String attrName : nameOfAttrs) {
			String fileName = String.format(destDirectory+"%06d_%02d_%s_.gz", nextFileNumber, i, attrName);
			GZIPOutputStream zip = new GZIPOutputStream(new FileOutputStream(new File(fileName)));
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(zip, "UTF-8"));
			currentOutputFiles.put(attrName, writer);
			i++;
			sizeOfCurrentFiles.put(attrName, new Integer(0));
		}
		nextFileNumber++;
	}

	private static void initializeAttr2ClassMap() {
		attr2Class.put("_id", Id.class.getCanonicalName());
		attr2Class.put("contributors", String.class.getCanonicalName());
		attr2Class.put("coordinates", Coordinates.class.getCanonicalName());
		attr2Class.put("created_at", String.class.getCanonicalName());
		attr2Class.put("favorited", String.class.getCanonicalName());
		attr2Class.put("geo", Coordinates.class.getCanonicalName());
		attr2Class.put("id", String.class.getCanonicalName());
		attr2Class.put("id_str", String.class.getCanonicalName());
		attr2Class.put("in_reply_to_screen_name", String.class.getCanonicalName());
		attr2Class.put("in_reply_to_status_id", String.class.getCanonicalName());
		attr2Class.put("in_reply_to_status_id_str", String.class.getCanonicalName());
		attr2Class.put("in_reply_to_user_id", String.class.getCanonicalName());
		attr2Class.put("in_reply_to_user_id_str", String.class.getCanonicalName());
		attr2Class.put("possibly_sensitive", String.class.getCanonicalName());
		attr2Class.put("possibly_sensitive_editable", String.class.getCanonicalName());
		attr2Class.put("retweet_count", Integer.class.getCanonicalName());
		attr2Class.put("retweeted", String.class.getCanonicalName());
		attr2Class.put("source", String.class.getCanonicalName());
		attr2Class.put("text", String.class.getCanonicalName());
		attr2Class.put("truncated", String.class.getCanonicalName());
		attr2Class.put("place", Place.class.getCanonicalName());
		attr2Class.put("entities", Entities.class.getCanonicalName());
		attr2Class.put("user", User.class.getCanonicalName());
	}
}
/*Id idObj = gson.fromJson(obj.get("_id"), Id.class);
String contributors = gson.fromJson(obj.get("contributors"), String.class);
Coordinates coordinates = gson.fromJson(obj.get("coordinates"), Coordinates.class);
String created_at = gson.fromJson(obj.get("created_at"), String.class);
String favorited = gson.fromJson(obj.get("favorited"), String.class);
Coordinates geo = gson.fromJson(obj.get("geo"), Coordinates.class);
String id = gson.fromJson(obj.get("id"), String.class);
String id_str = gson.fromJson(obj.get("id_str"), String.class);
String in_reply_to_screen_name = gson.fromJson(obj.get("in_reply_to_screen_name"), String.class);
String in_reply_to_status_id = gson.fromJson(obj.get("in_reply_to_status_id"), String.class);
String in_reply_to_status_id_str = gson.fromJson(obj.get("in_reply_to_status_id_str"), String.class);
String in_reply_to_user_id = gson.fromJson(obj.get("in_reply_to_user_id"), String.class);
String in_reply_to_user_id_str = gson.fromJson(obj.get("in_reply_to_user_id_str"), String.class);
String possibly_sensitive = gson.fromJson(obj.get("possibly_sensitive"), String.class);
String possibly_sensitive_editable = gson.fromJson(obj.get("possibly_sensitive_editable"), String.class);
String retweet_count = gson.fromJson(obj.get("retweet_count"), String.class);
String retweeted = gson.fromJson(obj.get("retweeted"), String.class);
String source = gson.fromJson(obj.get("source"), String.class);
String text = gson.fromJson(obj.get("text"), String.class);
String truncated = gson.fromJson(obj.get("truncated"), String.class);

Place place = gson.fromJson(obj.get("place"), Place.class);
Entities entities = gson.fromJson(obj.get("entities"), Entities.class);
User user = gson.fromJson(obj.get("user"), User.class);*/
/*System.out.println(idObj);

		System.out.println("contributors: " + contributors);
		System.out.println("coordinates: " + coordinates);
		System.out.println("created_at: " + created_at);

		System.out.println(entities);

		System.out.println("favorited: " + favorited);
		System.out.println("geo: " + geo);
		System.out.println("id: " + id);
		System.out.println("id_str: " + id_str);
		System.out.println("in_reply_to_screen_name: " + in_reply_to_screen_name);
		System.out.println("in_reply_to_status_id: " + in_reply_to_status_id);
		System.out.println("in_reply_to_status_id_str: " + in_reply_to_status_id_str);
		System.out.println("in_reply_to_user_id: " + in_reply_to_user_id);
		System.out.println("in_reply_to_user_id_str: " + in_reply_to_user_id_str);
		System.out.println("place: " + place);
		System.out.println("possibly_sensitive: " + possibly_sensitive);
		System.out.println("possibly_sensitive_editable: " + possibly_sensitive_editable);
		System.out.println("retweet_count: " + retweet_count);
		System.out.println("retweeted: " + retweeted);
		System.out.println("source: " + source);
		System.out.println("text: " + text);
		System.out.println("truncated: " + truncated);

		System.out.println(user);*/

