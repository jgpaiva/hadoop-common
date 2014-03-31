import com.google.gson.JsonArray;

public class Coordinates {
	public String type;
	public JsonArray coordinates;

	@Override
	public String toString() {
		return "coordinates: [type=" + type + ", coordinates=" + coordinates
				+ "]";
	}
}
