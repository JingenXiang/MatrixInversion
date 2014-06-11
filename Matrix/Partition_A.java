package matrix;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.*;
import java.lang.*;
import java.math.*;
import java.io.*;
import java.net.*;
import java.nio.*;

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

public class Partition_A {
    private String path;
    private int limit, nReducer, nPartition, nPar, No, I0 = 0, I1 = 1;
    private FSDataOutputStream out_a1, out_a2, out_a3, out_a4;
	private String[] file2, file3, file4;
	private int[][] head2, head3, head4;
    private FSDataInputStream in;
    private Configuration conf;
    private FileSystem fs;
    private Partition_A p = null;
   
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
		int i, j, k, l, m, n, M, N, jj, nRed;
		int i1, i2, j1, j2, f1, f2, n1, n2, nrows_per_No;
		String file;
	
		M = jj2 - jj1;
		N = M - M / 2;
		jj = jj1 + M / 2;
		nrows_per_No = get_number_per_division(M / 2, nPar);

		/* A1: from nPartition tasks
		 */
		if(M <= limit) {
			if(No == 0) {
				out_a1 = fs.create(new Path(path + "/a.txt"));
				
				out_a1.writeInt(jj1);
				out_a1.writeInt(jj2);
				out_a1.writeInt(jj1);
				out_a1.writeInt(jj2);
				
				for(i = 0; i < nPartition; i++)
					out_a1.writeBytes(path + "/A." + i + "\n");
				out_a1.close();
			}

			out_a1 = fs.create(new Path(path + "/A." + No));
			n = get_number_per_division(M, nPartition);
			i1 = jj1 + n * No;
			i2 = i1 + n;
			if(i2 > jj2) i2 = jj2;
			j1 = jj1;
			j2 = jj2;	  
			out_a1.writeInt(i1);
			out_a1.writeInt(i2);
			out_a1.writeInt(j1);
			out_a1.writeInt(j2); 
			
			return;
		}
	    	
		/* A2: partitioned into 1 x n1
		 *     each file consists of max(nPartition / 2, 1) file
		 */	
		if(No < nPar) {
			file = path + "/A2/A.";
			nRed = get_number_reducer(nReducer, "A2");
			n = get_number_per_division(N, nRed);
			if(No == 0) {		
				for(i = 0; i < nRed; i++) {		
					out_a2 = fs.create(new Path(file + i));		    
					i1 = jj1;
					i2 = jj;
					j1 = jj + i * n;
					j2 = j1 + n;
					if(j2 > jj2) j2 = jj2;
					
					out_a2.writeInt(i1);
					out_a2.writeInt(i2);
					out_a2.writeInt(j1);
					out_a2.writeInt(j2);
					for(j = 0; j < nPar; j++) {
						out_a2.writeBytes(file + i + "." + j + "\n");
					}
					out_a2.close();
				}
			}
			
			file2 = new String[nRed];
			head2 = new int[nRed][4];
			for(i = 0; i < nRed; i++) {					
				file2[i] = file + i + "." + No;
				i1 = jj1 + No * nrows_per_No;
				i2 = i1 + nrows_per_No;
				if(i2 > jj) i2 = jj;
				j1 = jj + i * n;
				j2 = j1 + n;
				if(j2 > jj2) j2 = jj2;
				
				head2[i][0] = i1;
				head2[i][1] = i2;
				head2[i][2] = j1;
				head2[i][3] = j2;
			} 
			
			if(nPartition > 1) return;
		}

		/* A3 is partitioned into n2 x 1
		 * n2 = nReducer - nReducer / 2
		 * nPar tasks (nPar <= n2 = nRed)
		 */
		int nRed_per_Par, file_no = No - nPartition / 2;
		nRed = get_number_reducer(nReducer, "A3");
		nRed_per_Par = get_number_per_division(nRed, nPar);
		n = get_number_per_division(N, nPar);
		k = get_number_per_division(n, nRed_per_Par);
		file3 = new String[nRed_per_Par];
		head3 = new int[nRed_per_Par][4];
		for(i = 0; i < nRed_per_Par; i++) {			
			file = path + "/A3/A." + (file_no * nRed_per_Par + i);
			out_a3 = fs.create(new Path(file));
			i1 = jj + file_no * n + i * k;
			i2 = i1 + k;
			if(i2 > jj + (file_no + 1) * n) i2 = jj + (file_no + 1) * n;
			if(i2 > jj2) i2 = jj2;
			j1 = jj1;
			j2 = jj;
			out_a3.writeInt(i1);
			out_a3.writeInt(i2);
			out_a3.writeInt(j1);
			out_a3.writeInt(j2);
			out_a3.writeBytes(file + ".0\n");
			out_a3.close();
			
			file3[i] = file + ".0";
			head3[i][0] = i1;
			head3[i][1] = i2;
			head3[i][2] = j1;
			head3[i][3] = j2;
		}   

		/* A4 is partitioned into f2 x f1
		 * f2: the number of A4 rows being partitioned
		 * f1:  the number of A4 columns being partitioned
		 */ 
		f1 = get_factor(nReducer); 
		f2 = nReducer / f1;
		
		n1 = get_number_per_division(N, f1);
		n2 = get_number_per_division(N, f2);
		int n_per_f2;
		
		file_no = No - nPartition / 2;
		file = path + "/A4/A.";
		if(nPar > f2) {
			n_per_f2 = get_number_per_division(nPar, f2);
			if(file_no % n_per_f2 == 0) {
				for(i = 0; i < f1; i++) {
					k = (file_no / n_per_f2) * f1 + i;
					out_a4 = fs.create(new Path(file + k));
					i1 = jj + (file_no / n_per_f2) * n2;
					i2 = i1 + n2;
					if(i2 > jj2) i2 = jj2;
					j1 = jj + i * n1;
					j2 = j1 + n1;
					if(j2 > jj2) j2 = jj2;
					out_a4.writeInt(i1);
					out_a4.writeInt(i2);
					out_a4.writeInt(j1);
					out_a4.writeInt(j2);
					
					for(j = 0; j < n_per_f2; j++) 
						out_a4.writeBytes(file + k + "." + j + "\n");
					out_a4.close();
				}
			}

			file4 = new String[f1];
			head4 = new int[f1][4];
			for(i = 0; i < f1; i++) {
				k = (file_no / n_per_f2) * f1 + i;
				j = file_no % n_per_f2;
				i1 = jj + nrows_per_No * file_no;
				i2 = i1 + nrows_per_No;
				if(i2 > jj2) i2 = jj2;
				j1 = jj + i * n1;
				j2 = j1 + n1;
				if(j2 > jj2) j2 = jj2;

				file4[i] = file + k + "." + j;
				head4[i][0] = i1;
				head4[i][1] = i2;
				head4[i][2] = j1;
				head4[i][3] = j2;
			}
		} else { // f2 >= nPartition
			n_per_f2 = get_number_per_division(f2, nPar);
			n = get_number_per_division(N, nPar);
			k = get_number_per_division(n, n_per_f2);
			file4 = new String[n_per_f2 * f1];
			head4 = new int[n_per_f2 * f1][4];
			for(m = 0; m < n_per_f2; m++) {
				for(j = 0; j < f1; j++) {
					l = (file_no * n_per_f2 + m) * f1 + j;
					i = m * f1 + j;
					out_a4 = fs.create(new Path(file + l));		    
					i1 = jj + file_no * n + m * k;
					i2 = i1 + k;
					if(i2 > jj + (file_no + 1) * n) i2 = jj + (file_no + 1) * n;
					if(i2 > jj2) i2 = jj2;
					j1 = jj + j * n1;
					j2 = j1 + n1;
					if(j2 > jj2) j2 = jj2;
					out_a4.writeInt(i1);
					out_a4.writeInt(i2);
					out_a4.writeInt(j1);
					out_a4.writeInt(j2);
					out_a4.writeBytes(file + l + ".0\n");
					out_a4.close();
			
					file4[i] = file + l + ".0";
					head4[i][0] = i1;
					head4[i][1] = i2;
					head4[i][2] = j1;
					head4[i][3] = j2;
				}
			}
		}
    }	    

    public Partition_A(String path, int limit, int nReducer, 
					   int nPartition, int No) 
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
		this.nPartition = Math.max(1, nPartition);
		this.nPar = Math.max(1, nPartition / 2);
		this.No = No;
		
		set_partition(I0, I1);	
    }
	
    public Partition_A(String path, int limit, int nReducer, 
					   int nPartition, int No, int j1, int j2)  
		throws IOException {
		this.path = path;
		this.limit = limit;
		this.nReducer = nReducer;
		this.nPartition = Math.max(1, nPartition);
		this.nPar = Math.max(1, nPartition / 2);
		this.No = No;
		
		conf = new Configuration();
		fs = FileSystem.get(conf);
		
		set_partition(j1, j2);
    }

	public void close() {
	}

    // Save double d[], i0, size = i1 - i0
    // lines: line_number, d[],
    public void save_partition(double[][] d, int i0, int i1, int[] line_no)
		throws IOException {
		int i, j, k, m, n, l, x, x0, N, M, nRed, nRed_per_Par;
		int nPar = Math.max(1, nPartition / 2);

		M = i1 - i0;
		N = M - M / 2;

		/*  Save A1 if the size is lower than the BOUND
		 */
		if(M <= limit) {
			for(i = 0; i < M && i < line_no.length; i++) {
				out_a1.writeInt(line_no[i]);	    
				for(j = i0; j < i1; j++)
					out_a1.writeDouble(d[i][j]); 
			}
			out_a1.close();
			return;
		} 

		/* Save partitioned A2
		 */
		if(line_no[0] < i0 + M / 2) {
			for(i = 0; i < file2.length; i++) {
				out_a2 = fs.create(new Path(file2[i]));
				for(k = 0; k < 4; k++) out_a2.writeInt(head2[i][k]);
				x = 0;
				while(x < line_no.length && line_no[x] < head2[i][0]) x++;
				for(k = 0; k < head2[i][1] - head2[i][0]; k++) {
					out_a2.writeInt(line_no[x + k]);
					for(j = head2[i][2]; j < head2[i][3]; j++) 
						out_a2.writeDouble(d[x + k][j]);				
				}
				out_a2.close();
			}
		}

		/*  Save partitioned A3
		 *  nRed >= nPar
		 */
 		if(file3 != null) {
			for(i = 0; i < file3.length; i++) {
				out_a3 = fs.create(new Path(file3[i]));
				for(k = 0; k < 4; k++) out_a3.writeInt(head3[i][k]);
				x = 0;
				while(x < line_no.length && line_no[x] < head3[i][0]) x++;
				for(k = 0; k < head3[i][1] - head3[i][0]; k++) {
					out_a3.writeInt(line_no[x + k]);
					for(j = head3[i][2]; j < head3[i][3]; j++) 
						out_a3.writeDouble(d[x + k][j]);				
				}
				out_a3.close();
			}			
		}

		/*  Save partitioned A4
		 *  nRed >= nPar
		 */
 		if(file4 != null) {
			for(i = 0; i < file4.length; i++) {
				out_a4 = fs.create(new Path(file4[i]));
				for(k = 0; k < 4; k++) out_a4.writeInt(head4[i][k]);
				x = 0;
				while(x < line_no.length && line_no[x] < head4[i][0]) x++;
				for(k = 0; k < head4[i][1] - head4[i][0]; k++) {
					out_a4.writeInt(line_no[x + k]);
					for(j = head4[i][2]; j < head4[i][3]; j++) 
						out_a4.writeDouble(d[x + k][j]);				
				}
				out_a4.close();
			}			
		}
				
		/* Save partitioned A1
		 */
		if(line_no[0] < i0 + M / 2) {
			p = new Partition_A(path + "/A1", limit, nReducer, 
								nPar, No, i0, i0 + M / 2);
			p.save_partition(d, i0, i0 + M / 2, line_no);
		}
	} 
    
    // Read From path + /a.txt, Double
    // header: i0 i1 j0 j1
    // lines: i d[0] d[1] ... d[n - 1]
    public void save_all() throws IOException {
		int i, j;
		int M = I1 - I0;
		int nrows_per_par = get_number_per_division(M, nPartition);
				
		long off = (long)(No * nrows_per_par) * M * Double.SIZE;
		off = (off + (No * nrows_per_par + 4) * Integer.SIZE) / Byte.SIZE;
		int N = nrows_per_par;
		if(N + No * nrows_per_par > M) N = M - No * nrows_per_par;
	
		int bufferSize = M * N * (Double.SIZE / Byte.SIZE);
		bufferSize += N * (Integer.SIZE / Byte.SIZE);
		byte[] b = new byte[bufferSize];
		
		FSDataInputStream in = fs.open(new Path(path + "/a.txt"));
		try {
			in.readFully(off, b);
		} catch (IOException e) {
			System.out.println(e);
		}

		in.close();
		
		ByteBuffer buffer = ByteBuffer.wrap(b);	
		double[][] d = new double[N][M];
		int[] line_no = new int[N];
		
		for(i = 0; i < N; i++) {
			line_no[i] = buffer.getInt();
			for(j = 0; j < M; j++) 
				d[i][j] = buffer.getDouble();
		} 
		
		save_partition(d, I0, I1, line_no);
    } 
}
