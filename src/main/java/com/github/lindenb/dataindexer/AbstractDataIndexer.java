package com.github.lindenb.dataindexer;

import java.io.Closeable;
import java.io.IOException;


public abstract class AbstractDataIndexer<T>
	implements Closeable
	{
	
	//protected FileOutputStream indexFile;
	protected long numberOfItems=0L;
	//protected OffsetOutputStream dataOutput;
	public abstract void close() throws IOException;
	
	
	
	}
