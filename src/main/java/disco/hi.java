package disco;

import java.awt.List;
import java.util.ArrayList;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class hi {
	public static void main(String[] ars){
		ArrayList<Integer> a = new ArrayList<>();
		for(int i=0;i<10;i++){
			a.add((int)(Math.random()*i));
		}
		System.out.println(a);
		int[] del_zero_r = new int[a.size()];
		int i, j = 0;
		for (i = 0; i < a.size(); i++) {
			if (a.get(i) != 0)
				del_zero_r[i] = j++;
			else
				del_zero_r[i] = -1;
		}
		System.out.println(arrToString(del_zero_r));
		System.out.println(a.stream().filter((s)->(s!=0)).collect(Collectors.toList()));
		
	}

	private static String arrToString(int[] arr) {
		String ret = "";
		for (int d : arr) {
			ret += d + " ";
		}
		return ret;
	}

}
