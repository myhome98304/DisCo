package disco;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MakeMatrixData {
	public static void main(String[] args) throws IOException{
		String outputPath = args[0];
		int d=Integer.parseInt(args[1]);
		int sections = Integer.parseInt(args[2]);
		FileSystem fs = FileSystem.get(new Configuration());

		PrintWriter fw = new PrintWriter(new OutputStreamWriter(
				fs.create(new Path(outputPath))));
		
		
		ArrayList<Integer> row=new ArrayList<>(d);
		ArrayList<Integer> col=new ArrayList<>(d);
		for(int i=0;i<d;i++){
			row.add(i); 
			col.add(i);
		}
		Collections.shuffle(row);
		Collections.shuffle(col);

		
		int x = sections*(sections+1)/2;
		int i=0;
		int j=d/x;
		int q=1;
		while(i<d-sections){
			int l=i;
			for(;l<j;l++)
				for(int k=i;k<j;k++){
					if(Math.random()*5>1){
						fw.write(row.get(l)+" "+col.get(k)+"\n");
					}
				}
			q++;			
			i+=(q-1)*d/x;
			j+=q*d/x;
			
		}
		fw.close();
		
	}
}
