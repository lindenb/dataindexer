package com.github.lindenb.dataindexer;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;


public class PrimaryDataIndexWriter<T>
	extends AbstractDataIndexer<T>
	{
	private PrimaryConfig<T> config;
	private List<SecondaryDataWriter<T, ?>> secondaries=new ArrayList<SecondaryDataWriter<T,?>>();
	
	private long offset=0L;
	
	public PrimaryDataIndexWriter(PrimaryConfig<T> config)
		{
		this.config=config;
		}
	
	public PrimaryConfig<T> getConfig()
		{
		return config;
		}
	

	//private List<SecondaryDataIndexer<T, ?>> secondary=new ArrayList<SecondaryDataIndexer<T,?>>();
	private RandomAccessFile indexFile=null;
	private RandomAccessOutput dataOutput;
	
	private void ensureOpen() throws IOException
		{
		if(this.indexFile==null) indexFile=new RandomAccessFile(getConfig().getIndexFile(),"rw");
		if(this.dataOutput==null) this.dataOutput=getConfig().getRandomAccessFactory().openForWriting(getConfig().getDataFile());
		}
	
	public void addSecondary(SecondaryDataWriter<T, ?> db2)
		{
		db2.setOwner(this);
		this.secondaries.add(db2);
		}
	
	
	public void insert(T item)
		throws IOException
		{
		ensureOpen();
		
		indexFile.writeLong(this.offset);
		
		DataOutputStream daos=new DataOutputStream(dataOutput);
		getConfig().getDataBinding().writeObject(item,daos);
		daos.flush();
		
		for(SecondaryDataWriter<T, ?> sdw2:this.secondaries)
			{
			sdw2.put(item, this.offset);
			}
		
		++this.numberOfItems;
		offset=dataOutput.getOffset();
		}
	
	@Override
	public void close() throws IOException
		{
		if(this.indexFile!=null) { indexFile.close();}
		if(this.dataOutput!=null) { dataOutput.flush();dataOutput.close();}
		DataOutputStream daos=new DataOutputStream(new FileOutputStream(getConfig().getSummaryFile()));
		daos.writeLong(this.numberOfItems);
		daos.flush();
		daos.close();
		
		for(SecondaryDataWriter<T, ?> sdw2:this.secondaries)
			{
			sdw2.close();
			}
		
		}
	}
