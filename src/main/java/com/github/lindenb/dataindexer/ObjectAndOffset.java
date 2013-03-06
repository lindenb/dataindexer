package com.github.lindenb.dataindexer;

class ObjectAndOffset<T>
	{
	private T object;
	private long offset;
	public ObjectAndOffset(T object,long offset)
		{
		this.object=object;
		this.offset=offset;
		}

	public T getObject()
		{
		return object;
		}
	
	public long getOffset()
		{
		return offset;
		}
	

	}
