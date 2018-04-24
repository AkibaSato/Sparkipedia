package spark;

/**
 * Efficient class for determining duplicate integers. Uses 1/4 the memory of a HashSet (since HashSet uses 
 * Integer wrapper, which is 16 bytes) and runs in O(1) without amortization. Used to remove duplicated docIds
 * in our Reducer function.
 * @author dsmyda
 */

public class IDSet {
	
	private int[] arr;
	public final int INTEGER_SIZE = 32;
	
	public IDSet(int size){
		//Divide by 32 and round up, determines number of indices needed for our set
		this.arr = new int[(size + 31)  >> 5];
	}
	
	public boolean isSet(int x){
		//Determine array index (just integer / 32 rounded down)
		int i = (x > 0) ? x - 1 >> 5 : 0;
		int val = arr[i];
		int res = val | (0 | (1 << x - i - 1 * INTEGER_SIZE));
		return res == val;
	}
	
	public void set(int x){
		int i = (x > 0) ? x - 1 >> 5 : 0;
		int val = arr[i];
		int res = val | (0 | (1 << x - i - 1 * INTEGER_SIZE));
		arr[i] = res;
	}
}