/**
 * 
 */
package org.apache.hadoop.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

/**
 * @author chathuri
 *
 */
public class HackedObjectInputStream extends ObjectInputStream
{
	 public HackedObjectInputStream(InputStream in) throws IOException {
	        super(in);
	    }

	    @Override
	    protected ObjectStreamClass readClassDescriptor() throws IOException, ClassNotFoundException {
	        ObjectStreamClass resultClassDescriptor = super.readClassDescriptor();

	        if (resultClassDescriptor.getName().equals("block.distributor.IndexValueObject"))
	            resultClassDescriptor = ObjectStreamClass.lookup(org.apache.hadoop.util.IndexValueObject.class);

	        return resultClassDescriptor;
	    }
}
