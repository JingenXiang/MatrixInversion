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
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;

public class LUInverse {
    private static int nReducer2, nReducer, nRed_L, nRed_U, nL, nU;
    private static int nL_inv, nU_inv, nL_mult, nU_mult;
    private static int size;
 
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

    public static int get_matrix_size(String s) throws IOException {
		int N;
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in;

		if(fs.exists(new Path(s + "/l.txt"))) {
			in = fs.open(new Path(s + "/l.txt"));
			in.readInt();
			N = in.readInt();
			in.close();
			return(N);
		}

		return(get_matrix_size(s + "/OUT/"));
    }
   
    public LUInverse(String path, int nRed) throws IOException {
		size = get_matrix_size(path);
	
		nReducer = nRed;
		nReducer2 = nRed;
		nL = get_factor(nRed);
		nU = nRed / nL;
		
		while(size * (size / nL) > 1000 * 1024 * 1024) {    //  800 MB
			nL *= 2;
		} 
	
		nRed_L = nReducer / 2;
       	nRed_U = nReducer - nRed_L;
		//nL = get_factor(nReducer);
		nU = nReducer / nL;
	
		nL_inv = get_number_per_division(size, nRed_L);
		nU_inv = get_number_per_division(size, nRed_U);
		nL_mult = get_number_per_division(nRed_L, nL);
		nU_mult = get_number_per_division(nRed_U, nU);	
    }

    public static class LUInverse_Mapper
		extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {	
			Configuration conf = context.getConfiguration();
			String path = conf.get("PATH");
			int jj = Integer.parseInt(value.toString());
			int i, j, k, m, n, lu, nn, M;
			String s, slu;
			FileSystem fs = FileSystem.get(conf);
			
			int nRed = Integer.parseInt(conf.get("nReducer2"));
			if(jj >= nRed / 2) {
				jj -= nRed / 2;
				nRed = Integer.parseInt(conf.get("nRed_L"));
				lu = 0;
				slu = "l";
			} else {
				nRed = Integer.parseInt(conf.get("nRed_U"));
				lu = 1;
				slu = "u";
			}
			int size = Integer.parseInt(conf.get("size"));
	    
			String[] str;
			double[][] li, l;	    
			double dd, di;
			
			n = get_number_per_division(size, nRed);
			li = new double[n][size];
	    
			Read_LU in = new Read_LU(path, slu.charAt(0), conf);
			l = new double[in.block_size][size];

			for(nn = 0; nn < size / in.block_size; nn++) {
				try {
					in.readBlock(l, nn, 0);
				} catch (IOException e) {
					System.out.println(e);
				}

				if(nn != 0) k = 0;
				else k = jj;
				
				for(i = k; i < in.block_size; i++) {
					int gi = nn * in.block_size + i;
					di = 1.0 / l[i][gi];		    
					
					for(j = 0; j < n && j * nRed + jj <= gi; j++) {
						if(j * nRed + jj == gi) li[j][gi] = di;
						else {
							dd = 0.0;
							for(m = j * nRed + jj; m < gi; m++) {
								dd -= l[i][m] * li[j][m];
							}
							li[j][gi] = dd * di;
						}
					}
				}	
			} 
			in.close();
	    
			// Save convert Li and Ui to files
			if((n - 1) * nRed + jj >= size) m = n - 1;
			else m = n;	
			String out_file = path + "/data/" + slu + "i." + jj;
			FSDataOutputStream out = fs.create(new Path(out_file));
			for(j = 0; j < m; j++) {
				k = jj + j * nRed;
				out.writeInt(k);
				out.writeInt(size - k);
				for(i = k; i < size; i++) {
					out.writeDouble(li[j][i]);
				}
			} 
			out.close(); 
			context.write(value, value);
		}	
    }

    public static class LUInverse_Reducer
		extends Reducer<Text, Text, Text, Text> {
		
		public static class readUi {
			int nRed_L, nRed_U, nU_mult, nU_inv, nL, nU, size;
			int M, N, n0, pos;
			FileSystem fs; 
			FSDataInputStream[] in;
			
			public readUi(String path, int jj, Configuration conf) 
				throws IOException {
				nU = Integer.parseInt(conf.get("nU"));
				nL = Integer.parseInt(conf.get("nL"));
				size = Integer.parseInt(conf.get("size"));
				nRed_U = Integer.parseInt(conf.get("nRed_U"));
				nRed_L = Integer.parseInt(conf.get("nRed_L"));
				nU_mult = Integer.parseInt(conf.get("nU_mult"));
				nU_inv = Integer.parseInt(conf.get("nU_inv"));
				
				fs = FileSystem.get(conf);
				N = get_number_per_division(size, nU);
				n0 = jj / nL;
				M = (size - n0) * N - (N - 1) * N * nU / 2;
				
				in = new FSDataInputStream[nU_mult];
				int m;
				for(m = 0; m < nU_mult && m * nU + n0 < nRed_U; m++) 
					in[m] = fs.open(new Path(path + "/ui." + (m * nU + n0)));
				
				pos = 0;
			}

			public int readLine(double[] Ui) throws IOException {
				int j, i0, nj = 0, m;		
				m = pos % nU_mult;
				if(m * nU + n0 >= nRed_U) return(-1);
				try {
					i0 = in[m].readInt();
					nj = in[m].readInt();
					for(j = size - nj; j < size; j++)
						Ui[j] = in[m].readDouble();
				} catch (IOException e) {
					System.out.println(e);
				}
				pos++;
				return(nj);
			}
		}

		public double[] read_Ui(String path, int jj, Configuration conf)
			throws IOException {
			int i, j, k, l, m, n0, n1, i0, j0, nj, N;
			int nRed_L, nRed_U, nU_mult, nU_inv, nL, nU, size;
			nU = Integer.parseInt(conf.get("nU"));
			nL = Integer.parseInt(conf.get("nL"));
			size = Integer.parseInt(conf.get("size"));
			nRed_U = Integer.parseInt(conf.get("nRed_U"));
			nRed_L = Integer.parseInt(conf.get("nRed_L"));
			nU_mult = Integer.parseInt(conf.get("nU_mult"));
			nU_inv = Integer.parseInt(conf.get("nU_inv"));
			
			/* nRed_L files are merged into nL files
			 * nL_mult files are merged into one;
			 * n0, n0 + nL, n0 + nL * 2, ...., n0 + nL * (nL_mult)
			 */ 
			N = get_number_per_division(size, nU);
			n0 = jj / nL;
			int M = (size - n0) * N - (N - 1) * N * nU / 2;
			double[] Ui = new double[M];
			
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream in;
			int Double_per_Byte = Double.SIZE / Byte.SIZE;
			int Int_per_Byte = Integer.SIZE / Byte.SIZE;
			
			int NN = size / nRed_U;
			int sSize = (size * (NN + 1) / 2 - n0 * NN) * Double_per_Byte;
			sSize += NN * 2 * Int_per_Byte;
			int off = nU * NN * Double_per_Byte;
			ByteBuffer[] buffer = new ByteBuffer[nU_mult];
			for(m = 0; m < nU_mult && m * nU + n0 < nRed_U; m++) {
				in = fs.open(new Path(path + "/ui." + (n0 + m * nU)));
				byte[] b = new byte[sSize - off * m];
				try {
					in.readFully(0, b);
				} catch (IOException e) {
					System.out.println(e);
				}

				in.close();
				buffer[m] = ByteBuffer.wrap(b);		
			}

			l = 0;
			for(i = 0; i < nU_inv; i++) {
				for(m = 0; m < nU_mult && m * nU + n0 < nRed_U; m++) {
					i0 = buffer[m].getInt();
					nj = buffer[m].getInt();		    
					for(j = size - nj; j < size; j++) {
						Ui[l++] = buffer[m].getDouble();
					}
				}
			}
			
			return(Ui);
		}

		public double[] read_Li(String path, int jj, Configuration conf)
			throws IOException {
			int i, j, k, l, m, n, n0, N, i0, nj;
			int nRed_L, nL_mult, nL, nU, size;
			nU = Integer.parseInt(conf.get("nU"));
			nL = Integer.parseInt(conf.get("nL"));
			size = Integer.parseInt(conf.get("size"));
			nRed_L = Integer.parseInt(conf.get("nRed_L"));
			nL_mult = Integer.parseInt(conf.get("nL_mult"));
			nL_inv = Integer.parseInt(conf.get("nL_inv"));
			
			n0  = jj % nL;
	    
			/* nRed_L files are merged into nL files
			 * nL_mult files are merged into one;
			 * n0, n0 + nL, n0 + nL * 2, ...., n0 + nL * (nL_mult)
			 */ 
			N = get_number_per_division(size, nL);
			int M = (size - n0) * N - (N - 1) * N * nL / 2;
			double[] Li = new double[M];
			
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream in;
			int Double_per_Byte = Double.SIZE / Byte.SIZE;
			int Int_per_Byte = Integer.SIZE / Byte.SIZE;

			int NN = size / nRed_L;
			int sSize = (size * (NN + 1) / 2 - n0 * NN) * Double_per_Byte;
			sSize += NN * 2 * Int_per_Byte;
			int off = nL * NN * Double_per_Byte;
			ByteBuffer[] buffer = new ByteBuffer[nL_mult];
			for(m = 0; m < nL_mult && m * nL + n0 < nRed_L; m++) {
				in = fs.open(new Path(path + "/li." + (n0 + m * nL)));
				byte[] b = new byte[sSize - off * m];
				try {
					in.readFully(0, b);
				} catch (IOException e) {
					System.out.println(e);
				}

				in.close();
				buffer[m] = ByteBuffer.wrap(b);		
			}

			l = 0;
			for(i = 0; i < nL_inv; i++) {
				for(m = 0; m < nL_mult && m * nL + n0 < nRed_L; m++) {
					i0 = buffer[m].getInt();
					nj = buffer[m].getInt();		    
					for(j = size - nj; j < size; j++) {
						Li[l++] = buffer[m].getDouble();
					}
				}
			}

			return(Li);
		}
	

		public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException , InterruptedException {
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			
			int i, j, k, m, n0, n1, jj, nL, nU, size;
			String file, path = conf.get("PATH");
			nU = Integer.parseInt(conf.get("nU"));
			nL = Integer.parseInt(conf.get("nL"));
			size = Integer.parseInt(conf.get("size"));

			jj = Integer.parseInt(key.toString());
			double[] Li = read_Li(path + "/data", jj, conf);
			//double[] Ui = read_Ui(path + "/data", jj, conf);
			double[] Ui = new double[size];
			double dd;
			n0 = jj / nL;
			n1 = jj % nL;
	    
			FSDataOutputStream out;
			file = "/Ai." + n0 + "." + n1;
			out = fs.create(new Path(path + "/data/" + file));
			// header = 0:N:0:N:nL:offset
			out.writeBytes("0:" + size + ":0:" + size);
			out.writeBytes(":" +  nL + ":" + n1 + "\n");
			
			int size_u = size - n0;
			int size_l = size - n1;
			int pos_u, pos_l, len_u, len_l, su, sl;
			
			readUi rui = new readUi(path + "/data", jj, conf);
			for(i = 0; i < size / nU; i++) {
				len_u = rui.readLine(Ui);
				pos_u = size;
				//pos_u = size_u * (i + 1) - i * (i + 1) * nU / 2;
				//len_u = size_u - nU * i;
				out.writeBytes((i * nU + n0) + ":");
				for(j = 0; j < size / nL; j++) {
					pos_l = size_l * (j + 1) - j * (j + 1) * nL / 2;
					len_l = size_l - nL * j;
					k = Math.min(len_l, len_u);		    
					sl = pos_l - k;
					dd = 0.0;	
					for(su = pos_u - k; su < pos_u; su++) 
						dd += Ui[su] * Li[sl++];
					
					if(j != size / nL - 1) out.writeBytes(dd + " "); 
					else out.writeBytes(dd + "\n"); 
				}   	    
			}

			context.write(key, key);
			out.close();    
		} 	
    }

    public static class LUPartitioner extends Partitioner<Text, Text> {
		@Override
			public int getPartition(Text key, Text value, int numPartitions) {
			if(key.getLength() == 0) return(0);
			return(Integer.parseInt(key.toString()));
		}
    }
    
    public void compute(String path) 
		throws Exception {
		Configuration conf = new Configuration();
		conf.set("PATH", path);
		conf.set("nReducer", nReducer2 + "");
		conf.set("nReducer2", nReducer + "");
		conf.set("nRed_L", nRed_L + "");
		conf.set("nRed_U", nRed_U + "");
		conf.set("nL", nL + "");
		conf.set("nU", nU + "");
		conf.set("nL_inv", nL_inv + "");
		conf.set("nU_inv", nU_inv + "");
		conf.set("nL_mult", nL_mult + "");
		conf.set("nU_mult", nU_mult + "");
		conf.set("size", size + "");
		
		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream out;
		for(int i = 0; i < nReducer; i++) {
			out = fs.create(new Path(path + "/logs/LUAA/A."+ i));
			out.writeBytes(i + "");
			out.close();
		}

		Read_LU rm = new Read_LU(path, 'l', conf);
		conf.set("DEM", (rm.i2 - rm.i1) + "");
		rm.close();
		
		Job job = new Job(conf, "LU Inverse");
		job.setJarByClass(LUInverse.class);
		job.setMapperClass(LUInverse_Mapper.class);
		job.setReducerClass(LUInverse_Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setPartitionerClass(LUPartitioner.class);
		job.setNumReduceTasks(nReducer);
		FileInputFormat.addInputPath(job, new Path("matrix/logs/LUAA"));
		FileOutputFormat.setOutputPath(job, new Path("matrix/logs/lu_out"));
		int res = job.waitForCompletion(true)?0 : 1;
    }
}
