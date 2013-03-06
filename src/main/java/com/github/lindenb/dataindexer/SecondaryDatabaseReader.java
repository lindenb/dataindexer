package com.github.lindenb.dataindexer;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

public class SecondaryDatabaseReader<PRIMARY,T>
	extends AbstractDatabaseReader<ObjectAndOffset<T>,SecondaryConfig<PRIMARY,T>>
	{
	private PrimaryDatabaseReader<PRIMARY> owner;
	
	private static class FunctionAdapter<T>
		implements Function<ObjectAndOffset<T>>
		{
		private Function<T> delegate;
		FunctionAdapter(Function<T> delegate)
			{
			this.delegate=delegate;
			}
		@Override
		public int apply(ObjectAndOffset<T> key) throws IOException
			{
			return this.delegate.apply(key.getObject());
			}
		}
	
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

    private long upperBound(
    		long first,long last,
    		final T select) throws IOException
    	{
        long len = last - first;
        while (len > 0)
                {
                long half = len / 2;
                long middle = first + half;
                ObjectAndOffset<T> x= this.get(middle);
                if (getConfig().getComparator().compare(select,x.getObject())<0)
                        {
                        len = half;
                        }
                else
                        {
                        first = middle + 1;
                        len -= half + 1;
                        }
                }
        return first;
    	}
    
	
	public void forEach(
			T beginKey,
			T endKey,
			boolean includeLast,
			Function<T> callback
			) throws IOException
		{
		long N=lowerBound(beginKey);
		long M=upperBound(N,size(),endKey);
		super.apply(N, M, new FunctionAdapter<T>(callback));
		}
	
	
	}
