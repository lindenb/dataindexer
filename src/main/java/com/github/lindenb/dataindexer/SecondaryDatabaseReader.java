package com.github.lindenb.dataindexer;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Comparator;

public class SecondaryDatabaseReader<PRIMARY,T> implements Closeable
	{
	private SecondaryConfig<PRIMARY,T> config;
	private PrimaryDatabaseReader<PRIMARY> owner;
	long numberOfItems=0;
	RandomAccessFile indexFile;
	RandomAccessInput dataFile;
	private TupleBinding<ObjectAndOffset<T>> objectAndOffsetBinding=null;
	private Comparator<ObjectAndOffset<T>> objectAndOffsetComparator=null;

	
	
	public SecondaryDatabaseReader(
		PrimaryDatabaseReader<PRIMARY> owner,
		SecondaryConfig<PRIMARY,T> config
		) throws IOException
		{
		this.config=config;
		this.owner=owner;
		this.objectAndOffsetBinding=config.createObjectAndOffsetBinding();
		this.objectAndOffsetComparator=config.createObjectAndOffsetComparator();

		
		DataInputStream dis=new DataInputStream(new FileInputStream(getConfig().getSummaryFile()));
		this.numberOfItems=dis.readLong();
		dis.close();
		indexFile=new RandomAccessFile(getConfig().getIndexFile(),"r");
		this.dataFile=getConfig().getRandomAccessFactory().openForReading(getConfig().getDataFile());
		}
	
	public SecondaryConfig<PRIMARY,T> getConfig()
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
	
	private ObjectAndOffset<T> getObjectAndOffset(long idx)
		throws IOException
		{
		checkIndexInRange(idx);
		this.indexFile.seek(idx*8);
		
		long offset=indexFile.readLong();
		this.dataFile.seek(offset);
		DataInputStream dis=new DataInputStream(this.dataFile);
		return this.objectAndOffsetBinding.readObject(dis);
		}
	
	public T get(long idx) throws IOException
		{
		return getObjectAndOffset(idx).getObject();
		}
	
	@Override
	public void close() throws IOException
		{
		this.indexFile.close();
		this.dataFile.close();
		}
	
	protected  long lowerBound(final T object)
		throws IOException
		{
		return lowerBound(0L, this.size(), object);
		}
	
    /** C+ lower_bound */
    protected  long lowerBound(
                long first,
                long last,
                final T object
                ) throws IOException
        {
        long len = last - first;
        while (len > 0)
                {
                long half = len / 2;
                long middle = first + half;
                ObjectAndOffset<T> oao= getObjectAndOffset(middle);
                if ( getConfig().getComparator().compare(oao.getObject(), object) < 0  )
                        {
                        first = middle + 1;
                        len -= half + 1;
                        }
                else
                        {
                        len = half;
                        }
                }
        return first;
        }

	
	public void forEach(
			T beginKey,
			T endKey,
			boolean includeLast
			) throws IOException
		{
		long N=lowerBound(beginKey);
		while(N<size())
			{
			ObjectAndOffset<T> oao= getObjectAndOffset(N);
			int i=getConfig().getComparator().compare(oao.getObject(), endKey);
			if(i>0 || (i==0 && !includeLast)) break;
			++N;
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
