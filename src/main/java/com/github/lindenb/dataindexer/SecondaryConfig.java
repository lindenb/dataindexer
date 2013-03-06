package com.github.lindenb.dataindexer;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Comparator;

public class SecondaryConfig<PRIMARY, K> extends AbstractConfig
	{
	protected SecondaryKeyCreator<PRIMARY, K> keyCreator;
	private Comparator<K> comparator;
	private TupleBinding<K> dataBinding;
	
	public SecondaryKeyCreator<PRIMARY, K> getKeyCreator() {
		return keyCreator;
	}
	public void setKeyCreator(SecondaryKeyCreator<PRIMARY, K> keyCreator) {
		this.keyCreator = keyCreator;
	}
	public Comparator<K> getComparator() {
		return comparator;
	}
	public void setComparator(Comparator<K> comparator) {
		this.comparator = comparator;
	}
	
	public TupleBinding<K> getDataBinding() {
		return dataBinding;
	}
	
	public void setDataBinding(TupleBinding<K> dataBinding) {
		this.dataBinding = dataBinding;
	}
	
	TupleBinding<ObjectAndOffset<K>> createObjectAndOffsetBinding()
		{
		return new TupleBinding<ObjectAndOffset<K>>()
			{
			@Override
			public ObjectAndOffset<K> readObject(DataInputStream in) throws IOException
				{
				K object=getDataBinding().readObject(in);
				long offset=in.readLong();
				return new ObjectAndOffset<K>(object,offset);
				}
			@Override
			public void writeObject(final ObjectAndOffset<K> o, java.io.DataOutputStream out) throws IOException
				{
				getDataBinding().writeObject(o.getObject(), out);
				out.writeLong(o.getOffset());
				}
			};
		}
	
	Comparator<ObjectAndOffset<K>> createObjectAndOffsetComparator()
		{
		return new Comparator<ObjectAndOffset<K>>()
			{
			public int compare(
					final ObjectAndOffset<K> o1, 
					final ObjectAndOffset<K> o2)
				{
				int i= getComparator().compare(o1.getObject(), o2.getObject());
				if(i!=0) return i;
				return (o1.getOffset()==o2.getOffset()?0:o1.getOffset()<o2.getOffset()?-1:1);
				}
			};
		}
	}
