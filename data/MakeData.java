import java.io.*;
import java.util.*;
import java.lang.*;
import java.math.*;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class MakeData {
    public static void main(String[] args) 
	throws IOException {
	String os = "B_" + args[0] + ".txt";
	int N = Integer.parseInt(args[0]);
	Random r = new Random(new Date().getTime());

	FileOutputStream fw = new FileOutputStream(os);
	DataOutputStream out = new DataOutputStream(fw);
	
	out.writeInt(0);
	out.writeInt(N);
	out.writeInt(0);
	out.writeInt(N);

	int i, j;
	for(i = 0; i < N; i++) {
	    out.writeInt(i);
	    for(j = 0; j < N; j++)
		out.writeDouble(r.nextDouble());
	}

  	out.close();
    }	
}
