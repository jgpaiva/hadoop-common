
public class User {
	public String contributors_enabled;
	public String created_at;
	public String default_profile;
	public String description;
	public String favourites_count;
	public String follow_request_sent;
	public String followers_count;
	public String following;
	public String friends_count;
	public String geo_enabled;
	public String id;
	public String id_str;
	public String is_translator;
	public String lang;
	public String listed_count;
	public String location;
	public String name;
	public String notifications;
	public String profile_background_color;
	public String profile_background_image_url;
	public String profile_background_image_url_https;
	public String profile_background_tile;
	public String profile_image_url;
	public String profile_image_url_https;
	public String profile_link_color;
	public String profile_sidebar_border_color;
	public String profile_sidebar_fill_color;
	public String profile_text_color;
	public String profile_use_background_image;
	public String screen_name;
	public String show_all_inline_media;
	public String statuses_count;
	public String time_zone;
	public String url;
	public String utc_offset;
	public String verified;
	@Override
	public String toString() {
		return "user: [contributors_enabled=" + contributors_enabled
				+ ", created_at=" + created_at + ", default_profile="
				+ default_profile + ", description=" + description
				+ ", favourites_count=" + favourites_count
				+ ", follow_request_sent=" + follow_request_sent
				+ ", followers_count=" + followers_count + ", following="
				+ following + ", friends_count=" + friends_count
				+ ", geo_enabled=" + geo_enabled + ", id=" + id + ", id_str="
				+ id_str + ", is_translator=" + is_translator + ", lang="
				+ lang + ", listed_count=" + listed_count + ", location="
				+ location + ", name=" + name + ", notifications="
				+ notifications + ", profile_background_color="
				+ profile_background_color + ", profile_background_image_url="
				+ profile_background_image_url
				+ ", profile_background_image_url_https="
				+ profile_background_image_url_https
				+ ", profile_background_tile=" + profile_background_tile
				+ ", profile_image_url=" + profile_image_url
				+ ", profile_image_url_https=" + profile_image_url_https
				+ ", profile_link_color=" + profile_link_color
				+ ", profile_sidebar_border_color="
				+ profile_sidebar_border_color
				+ ", profile_sidebar_fill_color=" + profile_sidebar_fill_color
				+ ", profile_text_color=" + profile_text_color
				+ ", profile_use_background_image="
				+ profile_use_background_image + ", pprotected="
				+ ", screen_name=" + screen_name + ", show_all_inline_media="
				+ show_all_inline_media + ", statuses_count=" + statuses_count
				+ ", time_zone=" + time_zone + ", url=" + url + ", utc_offset="
				+ utc_offset + ", verified=" + verified + "]";
	}
}