package matrix;

import java.io.*;
import java.util.*;
import java.lang.*;
import java.math.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import java.nio.*;

public class Read_LU {
    public class matrix_coord {
		public int i1, i2, j1, j2;
    }

    public String file, path, root = "";
    public int i1, i2, j1, j2, size, pos, n, i, nReducer;
    public int block_size;
    public Configuration conf;
    public FileSystem fs;
    public FSDataInputStream in = null, in_L2 = null;
    public Read_LU rm_lu1 = null, rm_lu3 = null;
    public char c;
    public double[] dd = null;
    public int[] indx = null, inverse_indx = null, nPos = null;
    
    public int BlockSize(String s) throws IOException {
		if(fs.exists(new Path(s + "/l.txt"))) {
			FSDataInputStream in1;
			in1 = fs.open(new Path(s + "/l.txt"));
			int x0 = in1.readInt();
			int x1 = in1.readInt();
			in1.close();
			return(x1 - x0);
		}

		return(BlockSize(s + "/A1"));
    }

    public matrix_coord init_size(String s) throws IOException {
		FSDataInputStream in1;
		String line;
		String[] str;

		matrix_coord mcd = new matrix_coord();	

		if(fs.exists(new Path(s + "/l.txt"))) {
			in1 = fs.open(new Path(s + "/l.txt"));
			mcd.i1 = in1.readInt();
			mcd.i2 = in1.readInt();
			mcd.j1 = in1.readInt();
			mcd.j2 = in1.readInt();
			in1.close();
			return(mcd);
		}

		matrix_coord m1 = init_size(s + "/A1/");
		matrix_coord m2 = init_size(s + "/OUT/");
		mcd.i1 = m1.i1;
		mcd.i2 = m2.i2;
		mcd.j1 = m1.j1;
		mcd.j2 = m2.j2;
		return(mcd);
    }
	
    public static int[] read_index(String path, Configuration conf) 
		throws IOException {
		int i, j;
		FileSystem fs = FileSystem.get(conf);
		int[] indx;

		if(fs.exists(new Path(path + "/index.txt"))) {
			FSDataInputStream in = fs.open(new Path(path + "/index.txt"));
			indx = new int[in.readInt() + 1];
			for(i = 0; i < indx.length; i++)
				indx[i] = in.readInt();
			in.close();
			return(indx);
		}
		int[] i1 = read_index(path + "/A1", conf);
		int[] i2 = read_index(path + "/OUT", conf);
		
		indx = new int[i1.length + i2.length - 1];
		j = i1.length;
		for(i = 0; i < j; i++)
			indx[i] = i1[i];
		
		for(i = 1; i < i2.length; i++)
			indx[i + j - 1] = i2[i] + i2[0] - i1[0];
		
		return(indx);
    }
   
    public void setMatrix(String s, char c1, Configuration config) 
		throws IOException {
		int t_i;
		
		root = s;
		conf = config;
		fs = FileSystem.get(conf);
		block_size = BlockSize(s);
		matrix_coord mcd = init_size(s);
		i1 = mcd.i1;
		i2 = mcd.i2;
		j1 = mcd.j1;
		j2 = mcd.j2;
		size = i2 - i1;
		pos = 0;
		nReducer = Integer.parseInt(conf.get("nReducer"));
		nReducer /= 2;
		n = (size - size / 2) / nReducer;
		if((size - size / 2) % nReducer != 0) n++;
		
		dd = new double[size];
		
		c = c1;
		if(c == 'l') {
			file = "/l.txt";
			path = "/L2/";
			indx = read_index(s, config);
		} else {
			file = "/u.txt";
			path = "/U2/";
			indx = new int[size + 1];
			for(t_i = 0; t_i <= size; t_i++) indx[t_i] = t_i - 1;
			indx[0] = 0;
		}
		
		inverse_indx = new int[size + 1];
		inverse_indx[0] = indx[0];
		for(t_i = 1; t_i <= size; t_i++)
			inverse_indx[indx[t_i] + 1] = t_i - 1;
		
		if(fs.exists(new Path(root + path + "/A.0"))) {
			nPos = new int[nReducer + 1];
			String L2_file = root + path + "/A.";
			for(t_i = 0; t_i < nReducer; t_i++) {
				in = fs.open(new Path(L2_file + t_i));
				nPos[t_i] = in.readInt();
				nPos[t_i + 1] = in.readInt();
				in.close();
			}
		}	

		if(fs.exists(new Path(root + "/A1"))) {
			rm_lu1 = new Read_LU(root + "/A1", c, config);
		}

		if(fs.exists(new Path(root + "/OUT"))) {
			rm_lu3 = new Read_LU(root + "/OUT", c, config);
		}
    }

    public Read_LU() {
		i = 0;
    }

    public Read_LU(String s, char c1, Configuration config) 
		throws IOException {
		setMatrix(s, c1, config);
    }
	
    public double[] readLine() 
		throws IOException {
		int t_i;
		
		if(rm_lu1 != null) {	   
			if(pos < size / 2) {
				pos++;
				return(rm_lu1.readLine());
			} else {
				pos++;
				int k = nPos.length - 2;
				t_i = indx[pos] - size / 2 + nPos[0];
				while(k >= 0 && nPos[k] > t_i) 
					k--;
				int off = t_i - nPos[k];
				
				long j = (Double.SIZE * (size / 2) + Integer.SIZE) * off;
				j = (j + 5 * Integer.SIZE) / Byte.SIZE;
				in_L2 = fs.open(new Path(root + path + "/A." + k));
				in_L2.seek(j);
				for(t_i = 0; t_i < size / 2; t_i++)
					dd[t_i] = in_L2.readDouble();
				in_L2.close();
				
				double[] d2 = rm_lu3.readLine();
				for(t_i = size / 2; t_i < pos; t_i++)
					dd[t_i] = d2[t_i - size / 2];
				
				return(dd);	
			}	    
		}

		if(pos == 0) {
			pos++;
			in = fs.open(new Path(root + file));
			for(t_i = 0; t_i < 5; t_i++)
				in.readInt();
			
			dd[0] = in.readDouble();
			return(dd);	     
		}

		if(pos >= size) return(null);
		
		pos++;
		in.readInt();
		for(t_i = 0; t_i < pos; t_i++)
			dd[t_i] = in.readDouble();
		
		return(dd);
    } 

    public void readBlock(double[][] A, int off_i, int off_j, String file, 
						  int length, int t_off, int t_l) 
		throws IOException {
		/* double[][] A, int off_i, int off_j,
		 * String file, int length, int t_off, int t_l
		 */
		FSDataInputStream inL2 = fs.open(new Path(file));
		long offset = off_i;
		offset *= (long)Double.SIZE * size / 2 + Integer.SIZE;
		offset = (offset + 4 * Integer.SIZE) / Byte.SIZE;
				
		int N = length;
		if(N + t_l > block_size) N = block_size - t_l;
		int bufferSize = N * size / 2;
		bufferSize = bufferSize * (Double.SIZE / Byte.SIZE);
		bufferSize += length * (Integer.SIZE / Byte.SIZE);
				
		byte[] b = new byte[bufferSize];
		inL2.readFully(offset, b);
		inL2.close();		
		ByteBuffer buffer = ByteBuffer.wrap(b);
				
		int t_i, t_j, t_k;	
		for(t_i = 0; t_i < length && t_l < block_size; t_i++) {
			buffer.getInt();
			t_l++;
			t_k = inverse_indx[t_off + t_l] - t_off;
			for(t_j = 0; t_j < size / 2; t_j++) 
						A[t_k][off_j + t_j] = buffer.getDouble();
		}
    }

    public void readBlock(double[][] A, int block_no, int off) 
		throws IOException {
		int t_i, t_j, t_l, t_off, nRed_size, bufferSize;
		String file_name;
		ByteBuffer buffer;

		if(rm_lu1 == null) { // only l.txt or u.txt
			in = fs.open(new Path(root + file));
			
			bufferSize = block_size * (block_size + 1) / 2; 
			bufferSize = bufferSize * (Double.SIZE / Byte.SIZE);
			bufferSize += (block_size + 4) * (Integer.SIZE / Byte.SIZE);
			byte[] b = new byte[bufferSize];
			in.readFully(0, b);
			in.close();
		
			buffer = ByteBuffer.wrap(b);

			for(t_i = 0; t_i < 4; t_i++)
				buffer.getInt();
			for(t_i = 0; t_i < block_size; t_i++) {
				buffer.getInt();
				for(t_j = 0; t_j <= t_i; t_j++) 
					A[t_i][t_j + off] = buffer.getDouble();
			}
			
			return;
		}

		/* Else there is LU1, LU2 and LU3, we need read data from them
		 */
		// read data from the first part.
		if(block_no * block_size < size / 2) {
			rm_lu1.readBlock(A, block_no, off);
			return;
		}
		
		// read data from LU3
		rm_lu3.readBlock(A, block_no - (size / 2) / block_size, off + size / 2);
		
		// read data from LU2
		t_i = block_no * block_size - size / 2 + nPos[0];
		int n0 = nPos.length - 2;
		while(n0 >= 0 && nPos[n0] > t_i) 
			n0--;
		int offset = t_i - nPos[n0];
		
		t_i = (block_no + 1) * block_size - size / 2 + nPos[0];
		int n1 = n0;
		while(n1 < nReducer && nPos[n1] < t_i)
			n1++;
		if(n1 > nReducer - 1) n1 = nReducer - 1;
		
		MultiThreadReadLU[] mtrlu = new MultiThreadReadLU[n1 - n0 + 1];
		t_l = 0;
		t_off = block_no * block_size;
		for(t_i = n0; t_i <= n1; t_i++) {
			file_name = root + path + "/A." + t_i;
			nRed_size = nPos[t_i + 1] - nPos[t_i];
			if(t_i == n0) nRed_size -= offset;
			else offset = 0;
			try {
				readBlock(A, offset, off, file_name, nRed_size, t_off, t_l);
			} catch (IOException e) {
				System.out.println(e);
			} 
			t_l += nRed_size;
		}
    } 	


    public class MultiThreadReadLU extends Thread {
		private  int off_i, off_j, t_off, t_l, length;
		private double[][] A;
		private String file;
		
		public MultiThreadReadLU(double[][] A, int off_i, int off_j,
								 String file, int length, int t_off, int t_l) {
			this.off_i = off_i;
			this.off_j = off_j;
			this.t_off = t_off;
			this.t_l = t_l;
			this.length = length;
			this.A = A;
			this.file = file;
		}
		
		public void run() {	    
			try {
				//readBlock(A, off_i, off_j, file, length, t_off, t_l);
				FSDataInputStream inL2 = fs.open(new Path(file));
				long offset = off_i;
				offset *= (long)Double.SIZE * size / 2 + Integer.SIZE;
				offset = (offset + 4 * Integer.SIZE) / Byte.SIZE;
				
				int N = length;
				if(N + t_l > block_size) N = block_size - t_l;
				int bufferSize = N * size / 2;
				bufferSize = bufferSize * (Double.SIZE / Byte.SIZE);
				bufferSize += length * (Integer.SIZE / Byte.SIZE);
				
				byte[] b = new byte[bufferSize];
				inL2.readFully(offset, b);
				inL2.close();		
				ByteBuffer buffer = ByteBuffer.wrap(b);
				
				int t_i, t_j, t_k;	
				for(t_i = 0; t_i < length && t_l < block_size; t_i++) {
					buffer.getInt();
					t_l++;
					t_k = inverse_indx[t_off + t_l] - t_off;
					for(t_j = 0; t_j < size / 2; t_j++) 
						A[t_k][off_j + t_j] = buffer.getDouble();
				}
			} catch (IOException e) {
				System.out.println(e);
			}
		}
    }

    public void readBlock_M(double[][] A, int block_no, int off) 
		throws IOException {
		int t_i, t_j, t_l, t_off, nRed_size, bufferSize;
		String file_name;
		ByteBuffer buffer;

		if(rm_lu1 == null) { // only l.txt or u.txt
			in = fs.open(new Path(root + file));
			
			bufferSize = block_size * (block_size + 1) / 2; 
			bufferSize = bufferSize * (Double.SIZE / Byte.SIZE);
			bufferSize += (block_size + 4) * (Integer.SIZE / Byte.SIZE);
			byte[] b = new byte[bufferSize];
			in.readFully(0, b);
			in.close();
		
			buffer = ByteBuffer.wrap(b);

			for(t_i = 0; t_i < 4; t_i++)
				buffer.getInt();
			for(t_i = 0; t_i < block_size; t_i++) {
				buffer.getInt();
				for(t_j = 0; t_j <= t_i; t_j++) 
					A[t_i][t_j + off] = buffer.getDouble();
			}
			
			return;
		}

		/* Else there is LU1, LU2 and LU3, we need read data from them
		 */
		// read data from the first part.
		if(block_no * block_size < size / 2) {
			rm_lu1.readBlock_M(A, block_no, off);
			return;
		}
		
		// read data from LU3
		rm_lu3.readBlock_M(A, block_no - (size / 2) / block_size, off + size / 2);
		
		// read data from LU2
		t_i = block_no * block_size - size / 2 + nPos[0];
		int n0 = nPos.length - 2;
		while(n0 >= 0 && nPos[n0] > t_i) 
			n0--;
		int offset = t_i - nPos[n0];
		
		t_i = (block_no + 1) * block_size - size / 2 + nPos[0];
		int n1 = n0;
		while(n1 < nReducer && nPos[n1] < t_i)
			n1++;
		if(n1 > nReducer - 1) n1 = nReducer - 1;
		
		MultiThreadReadLU[] mtrlu = new MultiThreadReadLU[n1 - n0 + 1];
		t_l = 0;
		t_off = block_no * block_size;
		for(t_i = n0; t_i <= n1; t_i++) {
			file_name = root + path + "/A." + t_i;
			nRed_size = nPos[t_i + 1] - nPos[t_i];
			if(t_i == n0) nRed_size -= offset;
			else offset = 0;
			mtrlu[t_i - n0] = new MultiThreadReadLU(A, offset, off, file_name, 
													nRed_size, t_off, t_l);
			mtrlu[t_i - n0].start();
			t_l += nRed_size;
		}

		try {
			for(t_i = n0; t_i <= n1; t_i++)
				mtrlu[t_i - n0].join();
		} catch (InterruptedException e) {
			System.out.println("Thread interrupted.");
		} 
    } 
    
    public void readBlock2(double[][] A, int block_no, int off) 
		throws IOException {
		int t_i, t_j, t_k, t_l, t_n, t_off, nRed_size;
		
		if(rm_lu1 == null) { // only l.txt or u.txt
			in = fs.open(new Path(root + file));
			for(t_i = 0; t_i < 4; t_i++)
				in.readInt();
			for(t_i = 0; t_i < block_size; t_i++) {
				in.readInt();
				for(t_j = 0; t_j <= t_i; t_j++) 
					A[t_i][t_j + off] = in.readDouble();
			}
			in.close();
			return;
		}
		
		/* Else there is LU1, LU2 and LU3, we need read data from them
		 */
		// read data from the first part.
		if(block_no * block_size < size / 2) {
			rm_lu1.readBlock2(A, block_no, off);
			return;
		}
		
		// read data from LU3
		rm_lu3.readBlock2(A, block_no - (size / 2) / block_size, 
						  off + size / 2);
		
		// read data from LU2
		t_i = block_no * block_size - size / 2 + nPos[0];
		int n0 = nPos.length - 2;
		while(n0 >= 0 && nPos[n0] > t_i) 
			n0--;
		long offset = t_i - nPos[n0];
		
		t_i = (block_no + 1) * block_size - size / 2 + nPos[0];
		int n1 = n0;
		while(n1 < nReducer && nPos[n1] < t_i)
			n1++;
		if(n1 > nReducer - 1) n1 = nReducer - 1;
		
		t_l = 0;
		t_off = block_no * block_size;
		
		for(t_n = n0; t_n <= n1; t_n++) {
			nRed_size = nPos[t_n + 1] - nPos[t_n];
			if(t_n == n0) nRed_size -= (int)offset;
			else offset = 0;	    
			offset *= (long)Double.SIZE * size / 2 + Integer.SIZE;
			offset = (offset + 4 * Integer.SIZE) / Byte.SIZE;
			
			in = fs.open(new Path(root + path + "/A." + t_n));
			in.seek(offset);	    
			for(t_i = 0; t_i < nRed_size && t_l < block_size; t_i++) {
				in.readInt();
				t_l++;
				t_k = inverse_indx[t_off + t_l] - t_off;
				for(t_j = 0; t_j < size / 2; t_j++) 
					A[t_k][off + t_j] = in.readDouble();
			}
			in.close();	
		}
    } 
	
    public void close() throws IOException {
		if(rm_lu3 != null) rm_lu3.close();
		if(rm_lu1 != null) rm_lu1.close();
		if(in != null) in.close();
		if(in_L2 != null) in_L2.close();
    }
}
