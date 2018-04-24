package spark;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SparkReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		StringBuilder sb = new StringBuilder(" -> ");
		

//		Set<String> valuesSet = new HashSet<>();
//		
//		for (Text value : values) {
//			valuesSet.add(value.toString());
//		}
//		for (String setValue : valuesSet) {
//			sb.append(setValue);
//			if (valuesSet.iterator().hasNext()) {
//				sb.append(", ");
//			}
//		}

		
		Iterator<Text> iter = values.iterator();
		IDSet iS = new IDSet(150000);
		while(iter.hasNext()) {
			String val = iter.next().toString();
			int x = parseInt(val);
			if(!iS.isSet(x)){
				sb.append(val);
				if(iter.hasNext()){
					//In the event that the iterator next value is a duplicate, we will end on a extra ,
					//this needs fixed.
					sb.append(", ");
				}
				iS.set(x);
			}
		}
		
		context.write(key, new Text(sb.toString()));
	}
	
	public static int parseInt( final String s ) {
	   	int num  = 0;
	    	final int len  = s.length( );
	    	num = '0' - s.charAt( 0 );

	    	// Build the number.
	    	int i = 1;
	    	while ( i < len )
			num = num*10 + '0' - s.charAt( i++ );
	    	return num;
	} 
}
