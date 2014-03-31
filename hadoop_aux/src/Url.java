
import java.util.List;

public class Url {
	public String display_url;
	public String expanded_url;
	public List<String> indices;
	public String url;
	
	@Override
	public String toString() {
		return "url: [display_url=" + display_url + ", expanded_url="
				+ expanded_url + ", indices=" + indices + ", url=" + url + "]";
	}
}
