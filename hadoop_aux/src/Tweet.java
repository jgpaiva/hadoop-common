
import java.util.List;

public class Tweet {
	public Id _id;
	public String text;
	public String contributors;
	public String coordinates;
	public String created_at;
	public Entities entities;
	public String favorited;
	public String geo;
	public String id;
	public String id_str;
	public String in_reply_to_screen_name;
	public String in_reply_to_status_id;
	public String in_reply_to_status_id_str;
	public String place;
	public String possibly_sensitive;
	public String possibly_sensitive_editable;
	public String retweet_count;
	public String retweeted;
	public String source;
	public String truncated;
	//public user user;
	public List<User> user;
}