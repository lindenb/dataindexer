package com.github.lindenb.dataindexer;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

public class PrimaryDatabaseReader<T> implements Closeable
	{
	private PrimaryConfig<T> config;
	long numberOfItems=0;
	RandomAccessFile indexFile;
	RandomAccessInput dataFile;
	public PrimaryDatabaseReader(PrimaryConfig<T> config) throws IOException
		{
		this.config=config;
		DataInputStream dis=new DataInputStream(new FileInputStream(getConfig().getSummaryFile()));
		this.numberOfItems=dis.readLong();
		dis.close();
		indexFile=new RandomAccessFile(getConfig().getIndexFile(),"r");
		this.dataFile=getConfig().getRandomAccessFactory().openForReading(getConfig().getDataFile());
		}
	
	public PrimaryConfig<T> getConfig()
		{
		return config;
		}
	
	public long size()
		{	
		return numberOfItems;
		}
	
	public T get(long idx) throws IOException
		{
		if(idx<0 || idx>=this.numberOfItems) throw new IndexOutOfBoundsException(
				"0<="+idx+"<"+size()
				);
		this.indexFile.seek(idx*8);
		
		long offset=indexFile.readLong();
		System.err.println("offset:"+offset);
		this.dataFile.seek(offset);
		DataInputStream dis=new DataInputStream(this.dataFile);
		return getConfig().getDataBinding().readObject(dis);
		}
	
	@Override
	public void close() throws IOException
		{
		this.indexFile.close();
		this.dataFile.close();
		}
	
	}
