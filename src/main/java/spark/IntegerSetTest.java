package spark;

/**
 * Quick sanity check for IntegerSet correctness. 
 * @author dsmyda
 *
 */

public class IntegerSetTest {

		public static void main(String[] args){
			IDSet s = new IDSet(1002220);
			s.set(1002219);
			System.out.println(s.isSet(1002219));		//true
			System.out.println(s.isSet(1002218));		//false
			
			System.out.println(s.isSet(0));				//false
			
			IDSet ts = new IDSet(5);
			ts.set(5);
			System.out.println(ts.isSet(5));			//false
			
			IDSet ts1 = new IDSet(0);
			System.out.println(ts1.isSet(1));			//Exception
			
		}
}
