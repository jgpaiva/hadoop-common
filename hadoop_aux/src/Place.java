
import com.google.gson.JsonArray;

public class Place {
	public BoundingBox bounding_box;

	public class BoundingBox {
		public String type;
		public JsonArray coordinates;
		@Override
		public String toString() {
			return "BoundingBox [type=" + type + ", coordinates=" + coordinates
					+ "]";
		}
	}

	@Override
	public String toString() {
		return "Place [boundingBox=" + bounding_box + "]";
	}
}
