package matrix;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.*;
import java.lang.*;
import java.math.*;
import java.io.*;
import java.net.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;

public class Partition {
    private String path;
    private int limit, nReducer, I0 = 0, I1 = 1;
    private FSDataOutputStream out_a1;
    private FSDataOutputStream[] out_a2, out_a3, out_a4;
    private FSDataInputStream in;
    private Configuration conf;
    private FileSystem fs;
   
    // For the partitioning of U nad L
    // U is partitioned into f1 (>= sqrt(n)) parts, while 
    // L is partitioned into f2 (n / f1) parts.
    public static int get_factor(int n) {
        int i, j;
        j = 1;
        for(i = 1; i <= Math.sqrt(n); i++)
            if(n % i == 0) j = i;
        return(n / j);
    }
    
    // divide N int to k parts
    public static int get_number_per_division(int N, int k) {
		int n = N / k;
		if(N % k != 0) n++;
		return(n);
    }

    public static int get_number_reducer(int nReducer, String A) {
		if(A.equals("A2"))
			return(nReducer / 2);
		else
			return(nReducer - nReducer / 2);
    }

    public void set_partition(int jj1, int jj2) 
		throws IOException {
		int i, j, k, n, M, N, jj;
		int i1, i2, j1, j2, f1, f2, n1, n2;
		
		M = jj2 - jj1;
		N = M - M / 2;
		jj = jj1 + M / 2;
		
		out_a1 = fs.create(new Path(path + "/A1/a.txt"));
		if(M <= limit) {
			out_a1.writeInt(jj1);
			out_a1.writeInt(jj2);
			out_a1.writeInt(jj1);
			out_a1.writeInt(jj2);
			return;
		} else {
			out_a1.writeInt(jj1);
			out_a1.writeInt(jj);
			out_a1.writeInt(jj1);
			out_a1.writeInt(jj);         
		}
	
		// f2: the number of A4 rows being partitioned
		// f1:  the number of A4 columns being partitioned
		f1 = get_factor(nReducer); 
		f2 = nReducer / f1;
		
		/* n1: the number of A2 column being partitioned
		 * n2: the number of A3 rows being partitioned
		 */
		n1 = get_number_reducer(nReducer, "A2");
		n2 = get_number_reducer(nReducer, "A3");
	    
		out_a2 = new FSDataOutputStream[n1];
		out_a3 = new FSDataOutputStream[n2];
		out_a4 = new FSDataOutputStream[nReducer];
	
		/* A2 is partitioned into 1 x n1
		 */
		for(i = 0; i < n1; i++) {
			out_a2[i] = fs.create(new Path(path + "/A2/A." + i));
			n = get_number_per_division(N, n1);
			j1 = jj + i * n;
			j2 = j1 + n;
			if(j2 > jj2) j2 = jj2;
	    
			out_a2[i].writeInt(jj1);
			out_a2[i].writeInt(jj);
			out_a2[i].writeInt(j1);
			out_a2[i].writeInt(j2);
		}

		/* A3 is partitioned into n2 x 1
		 */
		for(i = 0; i < n2; i++) {
			out_a3[i] = fs.create(new Path(path + "/A3/A." + i));
			n = get_number_per_division(N, n2);
			i1 = jj + i * n;
			i2 = i1 + n;
			if(i2 > jj2) i2 = jj2;
			out_a3[i].writeInt(i1);
			out_a3[i].writeInt(i2);
			out_a3[i].writeInt(jj1);
			out_a3[i].writeInt(jj);
		}

		/* A4 is partitioned into f2 x f1
		 */
		n1 = get_number_per_division(N, f1);
		n2 = get_number_per_division(N, f2);
		for(i = 0; i < f2; i++) {
			i1 = jj + i * n2;
			i2 = jj + (i + 1) * n2;
			if(i2 > jj2) i2 = jj2;
			
			for(j = 0; j < f1; j++) {
				k = i * f1 + j;
				out_a4[k] = fs.create(new Path(path + "/A4/A." + k));
				j1 = jj + j * n1;
				j2 = jj + (j + 1) * n1;
				if(j2 > jj2) j2 = jj2;
				
				out_a4[k].writeInt(i1);
				out_a4[k].writeInt(i2);
				out_a4[k].writeInt(j1);
				out_a4[k].writeInt(j2);
			}
		}
		
		FSDataOutputStream out;
		for(i = 0; i < nReducer; i++) {
			out = fs.create(new Path(path + "/AA/A." + i));
			out.writeBytes("" + i);
			out.close();
		}
    }	    
	
    public Partition(String path, int limit, int nReducer) 
		throws IOException {	    
		FSDataInputStream in;
		conf = new Configuration();
		fs = FileSystem.get(conf);
		String s;
		
		/* a.txt is a binary file storing original data with the below format
		 * 0  N  0  N
		 * line_no d[ln][0] d[ln][1] .........
		 */
		if(fs.exists(new Path(path + "/a.txt"))) {
			in = fs.open(new Path(path + "/a.txt"));
			I0 = in.readInt();
			I1 = in.readInt();
			in.close();
		} else { // /A4/A.* is also a binary file
			in = fs.open(new Path(path + "/A.0"));
			I0 = in.readInt();
			in.close();
			
			in = fs.open(new Path(path + "/A." + (nReducer - 1)));
			in.readInt();
			I1 = in.readInt();
			in.close();
		}
	
		this.path = path;
		this.limit = limit;
		this.nReducer = nReducer;
		
		set_partition(I0, I1);
    }
	
    public Partition(String path, int limit, int nReducer, 
					 int j1, int j2)  throws IOException {	
		this.path = path;
		this.limit = limit;
		this.nReducer = nReducer;
		
		conf = new Configuration();
		fs = FileSystem.get(conf);
		
		set_partition(j1, j2);
    }
	
    public void close() throws IOException {
		int i;
		if(out_a2 != null) {
			for(i = 0; i < out_a2.length; i++)
		out_a2[i].close();
			for(i = 0; i < out_a3.length; i++)
				out_a3[i].close();
			for(i = 0; i < out_a4.length; i++)
				out_a4[i].close();		
		}
				
		if(out_a1 != null) 
			out_a1.close();
    }

    // Save double d[], i0, size = i1 - i0
    // In order to void unnecessary copying, we only store the postion
    // of data in the original files.
    public void save_partition(String[][] file, int f1, int f2, int i0, int i1) 
		throws IOException {
		int i, j, k, n, M, nRed;
		int g1 = f1 / 2, g2 = f2 / 2;
		
		M = i1 - i0;
		
		/* Save partitioned A1
		 */
		if(f1 > 1) f1 /= 2;
		if(f2 > 1) f2 /= 2;
		for(i = 0; i < f2; i++) 
			for(j = 0; j < f1; j++)
				out_a1.writeBytes(file[i][j] + "\n");
		
		/* Save partitioned A2
		 */
		nRed = get_number_reducer(nReducer, "A2");
		n = get_number_per_division(nRed, f1);
		for(i = 0; i < nRed; i++) {
			for(j = 0; j < f2; j++) {
				out_a2[i].writeBytes(file[j][g1 + i / n] + "\n");
			}
		}
		
		/* Save partitioned A3
		 */
		nRed = get_number_reducer(nReducer, "A3");
		n = get_number_per_division(nRed, f2);
		for(i = 0; i < nRed; i++) {
			for(j = 0; j < f1; j++) {
				out_a3[i].writeBytes(file[g2 + i / n][j] + "\n");
			}
		}

		/* Save partitioned A4
		 */
		int ff1 = get_factor(nReducer);
		int ff2 = nReducer / ff1;
		int n1 = get_number_per_division(ff1, f1);
		int n2 = get_number_per_division(ff2, f2);
		
		for(i = 0; i < ff2; i++) {
			for(j = 0; j < ff1; j++) {
				k = i * ff1 + j;
				out_a4[k].writeBytes(file[g2 + i / n2][g1 + j / n1]);
				out_a4[k].writeBytes("\n");
			}
		}
    }
	
    public void save_all() throws IOException {
		int i, j, f1, f2;
		String[][] file;
		
		if(fs.exists(new Path(path + "/a.txt"))) {
			// Read From path + /a.txt, Double
			// header: i0:i1:j0:j1
			// lines: i:d[0] d[1] ... d[n - 1]
			if(path.equals("matrix")) {
				file = new String[1][1];
				file[0][0] = "matrix/a.txt";
				save_partition(file, 1, 1, I0, I1);
			} else {
				conf = new Configuration();
				fs = FileSystem.get(conf);
				FSDataInputStream in = fs.open(new Path(path + "/a.txt"));
				in.seek(4 * Integer.SIZE / Byte.SIZE);
				j = 0;
				while(in.readLine() != null)
					j++;
				f1 = get_factor(j);
				f2 = j / f1;
				in.seek(4 * Integer.SIZE / Byte.SIZE);
				file = new String[f2][f1];
				for(i = 0; i < f2; i++)
					for(j = 0; j < f1; j++) {
						file[i][j] = in.readLine();
					}
				in.close();
				
				save_partition(file, f1, f2, I0, I1);
			}	    
		} else {
			// Read From path + /OUT/, double
			// header: i0 i1 j0 j1
			// lines: 2 i d[0], d[1], ...., d[n * 2 - 2], d[n * 2 - 1];
			f1 = get_factor(nReducer);
			f2 = nReducer / f1;
			file = new String[f2][f1];
			for(i = 0; i < f2; i ++)
				for(j = 0; j < f1; j++)
					file[i][j] = path + "/A." + (i * f1 + j);
			
			save_partition(file, f1, f2, I0, I1);
		}
    }    
}
