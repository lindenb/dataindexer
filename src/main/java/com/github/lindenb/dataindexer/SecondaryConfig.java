package com.github.lindenb.dataindexer;

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
	
	}
