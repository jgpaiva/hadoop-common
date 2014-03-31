
import java.util.List;

public class UserMentions {
	public String id;
	public String id_str;
	public List<String> indices;
	public String name;
	public String screen_name;
	
	@Override
	public String toString() {
		return "UserMentions [id=" + id + ", id_str=" + id_str + ", indices="
				+ indices + ", name=" + name + ", screen_name=" + screen_name
				+ "]";
	}
}
