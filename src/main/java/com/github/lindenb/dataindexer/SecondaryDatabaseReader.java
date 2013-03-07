package com.github.lindenb.dataindexer;

import java.io.IOException;
import java.util.Comparator;

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
	
	@Override
	/** this is the sizeof a ObjectAndOffset= sizeof(object)+sizeof(long) */
	protected int getSizeOf()
		{
		return super.getSizeOf()+8;
		}	
	
	protected  long lower_bound(final T object)
		throws IOException
		{
		return lower_bound(0L, this.size(), object);
		}
	
	public boolean contains(final T object) throws IOException
		{
		return !equal_range(object).isEmpty();
		}
	
	public Interval equal_range(
			long first,
            long last,
            final T val
            ) throws IOException
		{	
	    

	        long len = last-first;
	        long half;
	        long middle, left, right;

	        while (len > 0)
	  	{
	  	  half = len /2;
	  	  middle = first;
	  	  middle+=half;
	  	  T at_mid=get(middle).getObject();
	  	  if (getConfig().getComparator().compare(at_mid, val) <0)
	  	    {
	  	      first = middle;
	  	      ++first;
	  	      len = len - half - 1;
	  	    }
	  	  else if (getConfig().getComparator().compare(val,at_mid) <0 )
	  	  	{
	  	    len = half;
	  	  	}
	  	  else
	  	    {
	  	      left =lower_bound(first, middle, val);
	  	     first+=len;
	  	      right = upper_bound(++middle, first, val);
	  	      return new Interval(left, right);
	  	    }
	  	}
	        return  new Interval(first, first);

		
		}
	
	public Interval equal_range(
            final T object
            ) throws IOException
		{
		return equal_range(0,size(),object);
		}
	
	
	
    /** C+ lower_bound */
    protected  long lower_bound(
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

    private long upper_bound(
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
		long N=lower_bound(beginKey);
		long M=upper_bound(N,size(),endKey);
		super.apply(N, M, new FunctionAdapter<T>(callback));
		}
	
	
	}
