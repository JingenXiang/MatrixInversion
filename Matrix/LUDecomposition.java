package matrix;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.*;
import java.lang.*;
import java.math.*;
import java.io.*;
import java.nio.*;
import java.text.*;

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

public class LUDecomposition extends Configured {
	public static int par_num = 128;
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

    public static void ludcmp(double[][] a, int[] indx) {
		int i, j, k, imax, n = a.length; 
		double big, temp, TINY = 1.0e-20;;

		for(j = 0; j < n; j++) {
			big = a[j][j];
			imax = j;
			for(i = j; i < n; i++) {
				if(a[i][j] > big) {
					imax = i;
					big = a[i][j];
				}
			}
			indx[j] = imax;
			if(Math.abs(a[imax][j]) > TINY) {
				if(imax != j) { // swap a[imax][] vs a[j][]
					for(i = 0; i < n; i++) {
						temp = a[j][i];
						a[j][i] = a[imax][i];
						a[imax][i] = temp;
					}
				}

				if(j < n - 1) { // scale a[j + 1 : N][j]
					if(Math.abs(a[j][j]) <= TINY) a[j][j] = TINY;
					temp = 1.0 / a[j][j];
					for(i = j + 1; i < n; i++)
						a[i][j] *= temp;
				}
			}
	    
			for(i = j + 1; i < n; i++) // update a[j + 1 : N][j + 1 : N]
				for(k = j + 1; k < n; k++)
					a[i][k] -= a[i][j] * a[j][k];
			
		}
		
		// construct permutation index
		int[] tmp = new int[n];
		
		for(i = 0; i < n; i++)
			tmp[i] = i;
		
		for(i = 0; i < n; i++) {
			j = tmp[i];
			tmp[i] = tmp[indx[i]];
			tmp[indx[i]] = j;
		}

		for(i = 0; i < n; i++)
			indx[i] = tmp[i];
    }

    public static void save_lu_indx(String path, double[][] a, int[] indx, 
									int[] off) throws IOException {
		int i, j, n = a.length;
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		FSDataOutputStream out = fs.create(new Path(path + "/l.txt"));
		out.writeInt(off[0]);
		out.writeInt(n + off[0]);
		out.writeInt(off[2]);
		out.writeInt(n + off[2]);
		for(i = 0; i < n; i++) {
			out.writeInt(i + off[0]);
			for(j = 0; j < i; j++) 
				out.writeDouble(a[i][j]);
			out.writeDouble(1.0);
		}
		out.close();
		//fs.replication
		
		out = fs.create(new Path(path + "/u.txt"));
		out.writeInt(off[0]);
		out.writeInt(n + off[0]);
		out.writeInt(off[2]);
		out.writeInt(n + off[2]);
		for(i = 0; i < n; i++) {
			out.writeInt(i + off[0]);
			for(j = 0; j <= i; j++) 
				out.writeDouble(a[j][i]);
		}
		out.close();

		out = fs.create(new Path(path + "/index.txt"));
		out.writeInt(indx.length);
		out.writeInt(off[0]);
		for(i = 0; i < n; i++) 
			out.writeInt(indx[i]);
		out.close();

		fs.setReplication(new Path(path + "/l.txt"), (short)20);
		fs.setReplication(new Path(path + "/u.txt"), (short)20);
		fs.setReplication(new Path(path + "/index.txt"), (short)20);
    }
	
    public static int[] get_matrix_header(String path, int nReducer) 
		throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in;
		int[] h = new int[4];
		
		if(nReducer == 0) { // get header from single file
			in = fs.open(new Path(path));
			h[0] = in.readInt();
			h[1] = in.readInt();
			h[2] = in.readInt();
			h[3] = in.readInt();	
			in.close();
		} else if (fs.exists(new Path(path + "/a.txt"))) {
			in = fs.open(new Path(path + "/a.txt"));
			h[0] = in.readInt();
			h[1] = in.readInt();
			h[2] = in.readInt();
			h[3] = in.readInt();	
			in.close();
		} else if(fs.exists(new Path(path + "/A.0"))) {
			in = fs.open(new Path(path + "/A.0"));
			h[0] = in.readInt();
			in.readInt();
			h[2] = in.readInt();
			in.close();
			in = fs.open(new Path(path + "/A." + (nReducer - 1)));
			in.readInt();
			h[1] = in.readInt();
			in.readInt();
			h[3] = in.readInt();
			in.close();
		} else {
			h[0] = 0;
			h[1] = 10000000;
		}
		
		return(h);
    }
  
    public static int get_matrix_size(String path, int nReducer) 
		throws IOException {
		int[] h = get_matrix_header(path, nReducer);
		return(h[1] - h[0]);
    }
	
    /* Read data from "file" to A, which start the (i0, j0).
     * Using information in "file" to determine which data should
     * be read into A.
     */
    public static void read_matrix(double[][] A, int i0, int j0,
								   char T, String file)
		throws IOException {
		int i, j, i1, j1;
		int x0, x1, y0, y1, xm, yn, xst, xsp, yst, ysp;
		
		if(T == 'T') {
			i1  = i0 + A[0].length;
			j1 = j0 + A.length;			
		} else {
			i1  = i0 + A.length;
			j1 = j0 + A[0].length;
		}

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = fs.open(new Path(file));;
		x0 = in.readInt();
		x1 = in.readInt();
		y0 = in.readInt();
		y1 = in.readInt();
		xm = x1 - x0;
		yn = y1 - y0;
		
		if(xm <= 0 || yn <= 0) {
			in.close();
			return;
		}

		xst = Math.max(i0, x0);
		xsp = Math.min(i1, x1);
		yst = Math.max(j0, y0);
		ysp = Math.min(j1, y1);
		
		if(xst >= xsp || yst >= ysp) {
			in.close();
			return;
		}

		int bufferSize = xm * yn * (Double.SIZE / Byte.SIZE);
		bufferSize += (xm + 4) * (Integer.SIZE / Byte.SIZE);
		byte[] b = new byte[bufferSize];
		in.readFully(0, b);
		in.close();
		ByteBuffer buffer = ByteBuffer.wrap(b);
		
		int off;
		int Int_per_Byte = Integer.SIZE / Byte.SIZE;
		int Double_per_Byte = Double.SIZE / Byte.SIZE;
		if(T == 'T') {
			for(i = xst; i < xsp; i++) {
				off = Int_per_Byte * (i - x0 + 5);
				off += ((i - x0) * yn + (yst - y0)) * Double_per_Byte;
				for(j = yst; j < ysp; j++) {
					A[j - j0][i - i0] = buffer.getDouble(off);
					off += Double_per_Byte;
				}
			}	    
		} else {
			for(i = xst; i < xsp; i++) {
				off = Int_per_Byte * (i - x0 + 5);
				off += ((i - x0) * yn + (yst - y0)) * Double_per_Byte;
				for(j = yst; j < ysp; j++) {   
					A[i - i0][j - j0] = buffer.getDouble(off);
					off += Double_per_Byte;
				}
			}
		}
    }

    public static class MultiThreadReadMatrix extends Thread {
		private int i0, j0;
		private double[][] A;
		private char T;
		private String file;
		
		public MultiThreadReadMatrix(double[][] A, int i0, int j0,
									 char T, String file) {
			this.i0 = i0;
			this.j0 = j0;
			this.A = A;
			this.T = T;
			this.file = file;
		}
		
		public void run() {
			try {
				read_matrix(A, i0, j0, T, file);
			} catch (IOException e) {
				System.out.println(e);
			}
		}
    }

    // Matrix header: i0 i1 j0 j2
    public static double[][] read_matrix(String file, char T) 
		throws IOException {
		int i, j, k, M, N;
		double[][] A;
		String line;
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = fs.open(new Path(file));
		int[] h = new int[4];
		for(i = 0; i < 4; i++) 
			h[i] = in.readInt();
		ArrayList<String> al = new ArrayList<String>();
		while((line = in.readLine()) != null) 
			al.add(line);
		in.close();
		
		M = h[1] - h[0];
		N = h[3] - h[2];
		if(T == 'T') A = new double[N][M];
		else A = new double[M][N];
		
		MultiThreadReadMatrix[] mtrm = new MultiThreadReadMatrix[al.size()];
		for(i = 0; i < al.size(); i++) {
			mtrm[i] = new MultiThreadReadMatrix(A, h[0], h[2], T, al.get(i));
			mtrm[i].start();
		}
		
		try {
			for(i = 0; i < al.size(); i++)
				mtrm[i].join();
		} catch (InterruptedException e) {
			System.out.println("Thread interrupted.");
		}

		return(A);
    } 

    public static double[][] read_matrix2(String file, char T) 
		throws IOException {
		int i, j, k, M, N;
		double[][] A;
		String line;
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = fs.open(new Path(file));
		int[] h = new int[4];
		for(i = 0; i < 4; i++) 
			h[i] = in.readInt();
		M = h[1] - h[0];
		N = h[3] - h[2];
		if(T == 'T') A = new double[N][M];
		else A = new double[M][N];

		while((line = in.readLine()) != null) {
			try {
				read_matrix(A, h[0], h[2], T, line);
			} catch (IOException e) {
				System.out.println(e);
			}
		}

		in.close();

		return(A);
    } 

    /* Read A4 or A4 - L2 * U2
     * These Matrices are partitioned into f2 x f1
     */ 
    public static double[][] read_A4(String path, int nReducer) 
		throws IOException {
		int m, N;
		int[] h = get_matrix_header(path, nReducer);
		N = h[1] - h[0];
		double[][] M = new double[N][N];
		
		for(m = 0; m < nReducer; m++) {
			try {
				read_matrix(M, h[0], h[2], 'N', path + "/A." + m);
			} catch (IOException e) {
				System.out.println(e);
			}
		}
		
		return(M);
    }

    public static void save_matrix(String file, double[][] a, int i0, int j0)
		throws IOException {
		int i, j, M = a.length, N = M > 0 ? a[0].length : 0;
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream out = fs.create(new Path(file));
		
		out.writeInt(i0);
		out.writeInt(M + i0);
		out.writeInt(j0);
		out.writeInt(j0 + N);
		
		for(i = 0; i < M; i++) {
			out.writeInt(i + i0);
			
			for(j = 0; j < N; j++) {
				out.writeDouble(a[i][j]);
			}
		} 
		out.close();			
    }

    public static class LU_Mapper
		extends Mapper<Object, Text, Text, Text> {
		
		public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {	    
			int i, j, k, l, m, n, M, N;
			double dd;
			String buffer, s_lu = value.toString();
			
			double[][] A2, u2, l1;
			double[] out;
			int[] indx;
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			int nReducer = Integer.parseInt(conf.get("nReducer"));
			String path = conf.get("PATH");
			
			int lu = Integer.parseInt(s_lu);
			String file, out_file;
			char c_lu, trans;
			if(lu >= nReducer / 2) { // compute U2
				lu -= nReducer / 2;
				c_lu = 'l';
				file = "/A2/A." + lu;		
				out_file = "/U2/A." + lu;
				trans = 'T';
			} else { // compute L2
				c_lu = 'u';
				file = "/A3/A." + lu;		
				out_file = "/L2/A." + lu;
				trans = 'N';
			}
			
			int[] h = get_matrix_header(path + file, 0);
			if(trans == 'T') {
				k = h[0];
				h[0] = h[2];
				h[2] = k;
				k = h[1];
				h[1] = h[3];
				h[3] = k;
			}
	   		
			System.out.println(path + file);
			System.out.format("h[0] = %d, h[1] = %d", h[0], h[1]);
			System.out.format("h[2] = %d, h[3] = %d", h[2], h[3]);
	
			if(h[1] <= h[0] || h[3] <= h[2]) {
				u2 = new double[0][0];
				save_matrix(path + out_file, u2, h[1], h[3]);
				context.write(value, value);
				return;
			}

			Read_LU in = new Read_LU(path + "/A1", c_lu, conf);
			u2 = new double[h[1] - h[0]][h[3] - h[2]];
			l1 = new double[in.block_size][h[3] - h[2]];
			
			A2 = read_matrix2(path + file, trans);
			
			for(n = 0; n < A2[0].length / in.block_size; n++) {
				try {
					in.readBlock(l1, n, 0);
				} catch (IOException e) {
					System.out.println(e);
				}
		
				for(j = 0; j < in.block_size; j++) {
					m = in.block_size * n + j;
					l = in.indx[m + 1];
					for(i = 0; i < u2.length; i++) {
						dd = 0.0;
						for(k = 0; k < m; k++) 
							dd += l1[j][k] * u2[i][k];
						u2[i][m] = (A2[i][l] - dd) / l1[j][m];
					}
				}
			} 	    

			in.close();
			save_matrix(path + out_file, u2, h[0], h[2]);
			context.write(value, value);
		}
    }

    public static class LU_Reducer
		extends Reducer<Text, Text, Text, Text> {	
		public int init = 0;

		/* U2 is partitioned into "nRecude - nReducer / 2" blocks.
		 * But for the multiplication, the U2 should be partitioned 
		 * into f1 blocks, therefore (nRecude - nReducer / 2) / f1 blocks
		 * should be merged into one blocks
		 */
		public double[][] read_LU2(String path, int nReducer, int jj, 
								   int[] h, char c)
			throws IOException {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream in;
			
			int i, j, k, m, n, M, N, f, nRed;
			int i1, i2, j1, j2, n1, n2, i0, j0;
			String my_path = "";
			int[] h2;
			if(c == 'u') {
				f = get_factor(nReducer);
				nRed = get_number_reducer(nReducer, "A2");	    
				n = get_number_per_division(nRed, f);
				k = jj % f;
				my_path = path + "/U2/A.";

				h2 = get_matrix_header(path + "/U2", nRed);
				i0 = h[2];
				j0 = h2[2];		
				M = h[3] - h[2];
				N = h2[3] - h2[2];
			} else {
				f = nReducer / get_factor(nReducer);
				nRed = get_number_reducer(nReducer, "A3");	    
				n = get_number_per_division(nRed, f);	    
				k = jj / get_factor(nReducer);
				my_path = path + "/L2/A.";
				
				h2 = get_matrix_header(path + "/L2", nRed);
				i0 = h[0];
				j0 = h2[2];
				M = h[1] - h[0];
				N = h2[3] - h2[2];
			}
	    
			double[][] LU2 = new double[M][N];	    
			n1 = Math.max(0, (k - 4) * n);
			n2 = Math.min((k + 4) * n, nRed - 1);
			
			int nn = n2 - n1 + 1;
			for(m = 0; m < nn; m++) {
				try {
					read_matrix(LU2, i0, j0, 'N', my_path + (m + n1));
				} catch (IOException e) {
					System.out.println(e);
				}		
			}	   
			
			return(LU2);
		}

		public class readL2 {
			int n1, n2;	
			String my_path = "";
			
			public readL2(String path, int nReducer, int jj, int[] h)
				throws IOException {
				Configuration conf = new Configuration();
				FileSystem fs = FileSystem.get(conf);
				FSDataInputStream in;
				
				int i, j, k, m, n, M, N, f, nRed;
				int[] h2;

				f = nReducer / get_factor(nReducer);
				nRed = get_number_reducer(nReducer, "A3");	    
				n = get_number_per_division(nRed, f);	    
				k = jj / get_factor(nReducer);
				my_path = path + "/L2/A.";
				
				n1 = Math.max(0, (k - 4) * n);
				n2 = Math.min((k + 4) * n, nRed - 1);
				h2 = get_matrix_header(my_path + n1, 0);

				while(h2[1] <= h[0]) {
					n1++;
					h2 = get_matrix_header(my_path + n1, 0);
				}
				
				h2 = get_matrix_header(my_path + n2, 0);
				while(h2[0] >= h[1]) {
					n2--;
					h2 = get_matrix_header(my_path + n2, 0);
				}
			}
			
			public double[][] readBlock(int[] h, int n0) throws IOException {
				int[] h2 = get_matrix_header(my_path + n0, 0);
				int h1 = h2[1] <= h[1] ? h2[1] : h[1];
				int h0 = h2[0] <= h[0] ? h[0] : h2[0];
				int M = h1 - h0, N = h2[3] - h2[2];

				double[][] L2 = new double[M][N];

				try {
					read_matrix(L2, h0, h2[2], 'N', my_path + n0);
				} catch (IOException e) {
					System.out.println(e);
				}		

				return(L2);
			}	   
		}


		public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException , InterruptedException {

			Configuration conf = context.getConfiguration();
			String path = conf.get("PATH");
			int nReducer = Integer.parseInt(conf.get("nReducer"));
			FileSystem fs = FileSystem.get(conf);
			
			int i, j, k, jj, m, n, l = 0;
			double dd;
			jj = Integer.parseInt(key.toString());
			int[] h = get_matrix_header(path + "/A4/A." + jj, 0);
			double[][] A4 = read_matrix2(path + "/A4/A." + jj, 'N');	    
			double[][] U2 = read_LU2(path, nReducer, jj, h, 'u');
			//double[][] L2 = read_LU2(path, nReducer, jj, h, 'l');
			readL2 rl2 = new readL2(path, nReducer, jj, h);
						
			FSDataOutputStream out;
			out = fs.create(new Path(path + "/OUT/A." + jj));
			for(i = 0; i < 4; i++)
				out.writeInt(h[i]);

			m = U2[0].length;
			for(n = rl2.n1; n <= rl2.n2; n++) {
				double[][] L2 = rl2.readBlock(h, n);
				for(i = 0; i < L2.length; i++) {
					out.writeInt(h[0] + l);
					for(j = 0; j < U2.length; j++) {
						dd = 0.0;
						for(k = 0; k < m; k++) 
							dd += L2[i][k] * U2[j][k];
						out.writeDouble(A4[l][j] - dd);
					}
					l++;
				}
			}
			out.close();

			context.write(key, key);
		}
    }
    
    public static class MyPartitioner extends Partitioner<Text, Text> {
		@Override
			public int getPartition(Text key, Text value, int numPartitions) {
			if(key.getLength() == 0) return(0);
			return(Integer.parseInt(key.toString()));
		}
    }

    public static void compute_lu_a4(String path, int nReducer) 
		throws Exception {
		Configuration conf = new Configuration();
		conf.set("PATH", path);
		conf.set("nReducer", nReducer + "");
		
		Job job = new Job(conf, "Matrix Inverse");
		job.setJarByClass(Inverse.class);
		job.setMapperClass(LU_Mapper.class);
		job.setReducerClass(LU_Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setPartitionerClass(MyPartitioner.class);
		job.setNumReduceTasks(nReducer);
		FileInputFormat.addInputPath(job, new Path("matrix/logs/AA"));
		FileOutputFormat.setOutputPath(job, new Path(path + "/tmp_out"));
		int res = job.waitForCompletion(true)?0 : 1;	
    }

    public static void compute(String path, int limit, int nReducer) 
		throws Exception {

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
	
		if(get_matrix_size(path, nReducer) <= limit) {
			double[][] M;

			if(fs.exists(new Path(path + "/a.txt"))) 
				M = read_matrix(path + "/a.txt", 'N');
			else 
				M = read_A4(path, nReducer);
			
			int[] indx = new int[M.length];
			double dd = 1.0;
			int[] h = get_matrix_header(path, nReducer);
			ludcmp(M, indx);
			save_lu_indx(path, M, indx, h);
			return;
		} else if(!fs.exists(new Path(path + "/A1"))) {
			Partition p = new Partition(path, limit, nReducer);
			p.save_all();
			p.close(); 
		}

		compute(path + "/A1", limit, nReducer);
		compute_lu_a4(path,  nReducer); //write data to path/OUT
		compute(path + "/OUT", limit, nReducer);
    } 
	
    public static class Partition_Mapper
		extends Mapper <Object, Text, Text, Text> {
		
		public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			int nReducer = Integer.parseInt(conf.get("nReducer"));
			String path = conf.get("PATH");
			int limit = Integer.parseInt(conf.get("limit"));
			int No = Integer.parseInt(value.toString());
			
			/* If more than 16 nodes read the same file,
			 * the HDFS is not stable.
			 */
			Partition_A pa;
			int k = par_num;
			if(nReducer < k) 
				pa = new Partition_A(path, limit, nReducer, nReducer, No);
			else 
				pa = new Partition_A(path, limit, nReducer, k, No);
			
			pa.save_all();
			pa.close();
			
			context.write(value, value);
		}
    }

    public static class Partition_Reducer
		extends Reducer<Text, Text, Text, Text> {	
		public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException , InterruptedException {
			context.write(key, key);
		}
    }
    
    public static void partition(String path, int limit, int nReducer) 
		throws Exception {
		int i, j, k;
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream out;
		for(i = 0; i < nReducer; i++) {
			out = fs.create(new Path("matrix/logs/AA/A." + i));
			out.writeBytes("" + i);
			out.close();
		}

		for(i = 0; i < Math.min(par_num, nReducer); i++) {
			out = fs.create(new Path("matrix/logs/BB/A." + i));
			out.writeBytes("" + i);
			out.close();
		}
	
		conf.set("PATH", path);
		conf.set("nReducer", nReducer + "");
		conf.set("limit", limit + "");
		
		Job job = new Job(conf, "Matrix Partition");
		job.setJarByClass(Inverse.class);
		job.setMapperClass(Partition_Mapper.class);
		job.setReducerClass(Partition_Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setPartitionerClass(MyPartitioner.class);
		job.setNumReduceTasks(nReducer);
		FileInputFormat.addInputPath(job, new Path("matrix/logs/BB"));
		FileOutputFormat.setOutputPath(job, new Path("matrix/logs/partition"));
		int res = job.waitForCompletion(true)?0 : 1;	
    } 
}

