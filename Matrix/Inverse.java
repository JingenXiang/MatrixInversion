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
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;

public class Inverse extends Configured {
    public static void main(String[] args) throws Exception {
		int limit = Integer.parseInt(args[0]);
		int nReducer = Integer.parseInt(args[1]);
		
		LUDecomposition LUD = new LUDecomposition();
		LUD.partition("matrix", limit, nReducer);
		LUD.compute("matrix", limit, nReducer);

		LUInverse lu = new LUInverse("matrix", nReducer);
		lu.compute("matrix");

		System.exit(0);
    }        						
}
