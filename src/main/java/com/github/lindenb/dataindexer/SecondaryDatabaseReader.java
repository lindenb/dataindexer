package com.github.lindenb.dataindexer;

import java.io.IOException;
import java.util.Comparator;

public class SecondaryDatabaseReader<PRIMARY,T>
	extends AbstractDatabaseReader<ObjectAndOffset<T>,SecondaryConfig<PRIMARY,T>>
	{
	private PrimaryDatabaseReader<PRIMARY> owner;

	
	public SecondaryDatabaseReader(
		PrimaryDatabaseReader<PRIMARY> owner,
		SecondaryConfig<PRIMARY,T> config
		) throws IOException
		{
		super(config);
		this.owner=owner;
		}
	
	public PrimaryDatabaseReader<PRIMARY> getOwner()
		{
		return this.owner;
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
    	Comparator<T> cmp=getConfig().getComparator();
        long len = last - first;
        while (len > 0)
                {
                long half = len / 2;
                long middle = first + half;
                ObjectAndOffset<T> oao= get(middle);
                if ( cmp.compare(oao.getObject(), object) < 0  )
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
			ObjectAndOffset<T> oao= get(N);
			int i=getConfig().getComparator().compare(oao.getObject(), endKey);
			if(i>0 || (i==0 && !includeLast)) break;
			++N;
			}
		}
	
	
	}
