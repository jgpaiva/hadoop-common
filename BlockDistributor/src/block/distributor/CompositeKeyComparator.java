package block.distributor;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class CompositeKeyComparator extends WritableComparator
{
	protected CompositeKeyComparator() {
		super(CompositeKey.class, true);
		}
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
	 
	CompositeKey key1 = (CompositeKey) w1;
	CompositeKey key2 = (CompositeKey) w2;
	 
	// (first check on udid)
	int compare = key1.getHost().compareTo(key2.getHost());
	 
	if (compare == 0) {
	// only if we are in the same input group should we try and sort by value (group value specified by user)
	if((key1.getGroupValue()==null)&& (key2.getGroupValue()==null)){
		return 0;
	}
	return key1.getGroupValue().toLowerCase().compareTo(key2.getGroupValue().toLowerCase());
	}
	 
	return compare;
	}
}
