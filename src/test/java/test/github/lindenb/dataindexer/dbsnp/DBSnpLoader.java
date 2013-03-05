package test.github.lindenb.dataindexer.dbsnp;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import com.github.lindenb.dataindexer.PrimaryConfig;
import com.github.lindenb.dataindexer.PrimaryDataIndexWriter;
import com.github.lindenb.dataindexer.PrimaryDatabaseReader;
import com.github.lindenb.dataindexer.SecondaryConfig;
import com.github.lindenb.dataindexer.SecondaryDataWriter;
import com.github.lindenb.dataindexer.SecondaryKeyCreator;
import com.github.lindenb.dataindexer.TupleBinding;

public class DBSnpLoader
	{
	private File dbSnp137File;
	
	private static class Snp
		{
		int bin;
		String chrom;
		int chromStart;
		int chromEnd;
		int rs_id;
		@Override
		public String toString() {
			return "rs"+rs_id+" "+chrom+":"+chromStart+"-"+chromEnd;
			}
		}
	private static class SnpBinding
		implements TupleBinding<Snp>
		{
		@Override
		public Snp readObject(DataInputStream in) throws IOException
			{
			Snp snp=new Snp();
			snp.bin=in.readInt();
			snp.chrom=in.readUTF();
			snp.chromStart=in.readInt();
			snp.chromEnd=in.readInt();
			snp.rs_id=in.readInt();
			return snp;
			}
		@Override
		public void writeObject(final Snp o, DataOutputStream out) throws IOException {
			out.writeInt(o.bin);
			out.writeUTF(o.chrom);
			out.writeInt(o.chromStart);
			out.writeInt(o.chromEnd);
			out.writeInt(o.rs_id);
			}
		}
	
	
	public void test()
		throws IOException
		{
		PrimaryConfig<Snp> config=new PrimaryConfig<Snp>();
		config.setName("tmp.dbsnp");
		config.setHomeDirectory(this.dbSnp137File.getParentFile());
		config.setDataBinding(new SnpBinding());
		PrimaryDataIndexWriter<Snp> primaryWriter=new PrimaryDataIndexWriter<DBSnpLoader.Snp>( config );
		
		SecondaryConfig<Snp, Integer> cfg2=new SecondaryConfig<Snp, Integer>();
		cfg2.setComparator(new Comparator<Integer>() {
			@Override
			public int compare(Integer arg0, Integer arg1) {
				return arg0.compareTo(arg1);
				}
			});
		cfg2.setDataBinding(new TupleBinding<Integer>() {
			@Override
			public Integer readObject(DataInputStream in) throws IOException {
				return in.readInt();
				}
			@Override
			public void writeObject(Integer o, DataOutputStream out)
					throws IOException {
				out.writeInt(o);
				}
			});
		cfg2.setKeyCreator(new SecondaryKeyCreator<DBSnpLoader.Snp, Integer>()
			{
			@Override
			public Set<Integer> getSecondaryKeys(Snp t) {
				Set<Integer> S= new HashSet<Integer>(1);
				S.add(t.rs_id);
				return S;
				}
			});
		SecondaryDataWriter<Snp, Integer> rs2snp=new SecondaryDataWriter<DBSnpLoader.Snp, Integer>(cfg2);
		primaryWriter.addSecondary(rs2snp);
		
		
		long nLine=0;
		BufferedReader in=new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(this.dbSnp137File))));
		String line;
		Pattern tab=Pattern.compile("[\t]");
		while((line=in.readLine())!=null)
			{
			if(++nLine>1000000) break;
			if(nLine%10000==0) System.err.println("count:"+nLine);
			String tokens[]=tab.split(line);
			Snp snp=new Snp();
			snp.bin=Integer.parseInt(tokens[0]);
			snp.chrom=tokens[1];
			snp.chromStart=Integer.parseInt(tokens[2]);
			snp.chromEnd=Integer.parseInt(tokens[3]);
			snp.rs_id=Integer.parseInt(tokens[4].substring(2));
			primaryWriter.insert(snp);
			}
		in.close();
		primaryWriter.close();
		
		Random rand=new Random(System.currentTimeMillis());
		PrimaryDatabaseReader<Snp> primaryDatabaseReader=new PrimaryDatabaseReader<DBSnpLoader.Snp>(config);
		for(int i=0;i< 10;++i)
			{
			int index=rand.nextInt((int)primaryDatabaseReader.size());
			Snp snp=primaryDatabaseReader.get(index);
			}
		primaryDatabaseReader.size();
		
		}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception
		{
		DBSnpLoader app=new DBSnpLoader();
		app.dbSnp137File=new File("/home/lindenb/src/cardioserve/snp137.txt.gz");
		app.test();
		
		}

	}
