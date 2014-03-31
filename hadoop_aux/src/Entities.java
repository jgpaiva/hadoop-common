
import java.util.List;
import com.google.gson.JsonArray;

public class Entities {	
	public JsonArray hashtags;
	public List<Url> urls;
	public List<UserMentions> user_mentions;
	
	@Override
	public String toString() {
		return "entities: [hashtags=" + hashtags + ", urls=" + urls
				+ ", user_mentions=" + user_mentions + "]";
	}
}
