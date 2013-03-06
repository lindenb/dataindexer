package com.github.lindenb.dataindexer;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

public class AbstractDatabaseReader<T,CONFIG extends AbstractConfig<T>>
	implements Closeable
	{
	private CONFIG config;
	private long numberOfItems=0;
	protected RandomAccessFile indexFile=null;
	protected RandomAccessInput dataFile=null;
	protected  AbstractDatabaseReader(CONFIG config) throws IOException
		{
		this.config=config;
		DataInputStream dis=new DataInputStream(new FileInputStream(getConfig().getSummaryFile()));
		this.numberOfItems=dis.readLong();
		dis.close();
		}
	
	public boolean isOpen()
		{
		return dataFile!=null;
		}
	
	public void open() throws IOException
		{
		if(isOpen()) return;
		DataInputStream dis=new DataInputStream(new FileInputStream(config.getSummaryFile()));
		this.numberOfItems=dis.readLong();
		dis.close();
		if(this.numberOfItems<0L) throw new IOException("summary file corrupted.");
		if(!config.isFixedSizeof())
			{
			this.indexFile=new RandomAccessFile(getConfig().getIndexFile(),"r");
			}
		
		this.dataFile=getConfig().getRandomAccessFactory().openForReading(getConfig().getDataFile());
		}
	
	public CONFIG getConfig()
		{
		return config;
		}
	
	public long size()
		{	
		return numberOfItems;
		}
	
	private long checkIndexInRange(long idx)
		{
		if(idx<0 || idx>=this.size()) throw new IndexOutOfBoundsException(
				"0<="+idx+"<"+size()
				);
		return idx;
		}
	
	protected int getSizeOf()
		{
		return getConfig().getSizeOfItem();
		}
	
	protected boolean isFixedSizeOf()
		{
		return getConfig().isFixedSizeof();
		}
	
	private long getOffsetFromIndex(long idx)  throws IOException
		{
		checkIndexInRange(idx);
		if(isFixedSizeOf())
			{
			return idx*getSizeOf();
			}
		else
			{
			this.indexFile.seek(idx*8);
			return indexFile.readLong();
			}
		}
	
	T getItemFromOffset(long offset)  throws IOException
		{
		this.dataFile.seek(offset);
		DataInputStream dis=new DataInputStream(this.dataFile);
		return getConfig().getDataBinding().readObject(dis);
		}
	
	public T get(long idx) throws IOException
		{
		return getItemFromOffset(getOffsetFromIndex(idx));
		}
	
	@Override
	public void close() throws IOException
		{
		if(this.indexFile!=null)
			{
			this.indexFile.close();
			this.indexFile=null;
			}
		if(this.dataFile!=null)
			{
			this.dataFile.close();
			this.dataFile=null;
			}
		}
	
	public void forEach(
			long beginIndex,
			long endIndex,
			PrimaryForEach<T> callback
			) throws IOException
		{
		callback.onBegin();
		while(beginIndex<endIndex)
			{
			if( callback.apply(get(beginIndex))!=0)
				{
				break;
				}
			++beginIndex;
			}
		callback.onEnd();
		}
	
	}
