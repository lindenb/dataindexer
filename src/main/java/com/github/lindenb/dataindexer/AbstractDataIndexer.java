package com.github.lindenb.dataindexer;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;


public abstract class AbstractDataIndexer<T,CONFIG extends AbstractConfig<T>>
	implements Closeable
	{
	protected long numberOfItems=0L;
	private TupleBinding<T> objectAndOffsetBinding=null;
	private CONFIG config;
	protected AbstractDataIndexer(CONFIG cfg)
		{
		this.objectAndOffsetBinding=cfg.getDataBinding();
		this.config=cfg;
		}
	
	public abstract void close() throws IOException;
	
	public final CONFIG getConfig()
		{
		return this.config;
		}
	
	protected final TupleBinding<T> getDataBinding()
		{
		return objectAndOffsetBinding;
		}
	
	protected void writeSummary()
		throws IOException
		{
		DataOutputStream daos=new DataOutputStream(new FileOutputStream(getConfig().getSummaryFile()));
		daos.writeLong(this.numberOfItems);
		daos.flush();
		daos.close();
		}
	}
