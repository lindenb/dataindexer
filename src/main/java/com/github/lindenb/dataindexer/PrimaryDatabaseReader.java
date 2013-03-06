package com.github.lindenb.dataindexer;

import java.io.Closeable;
import java.io.IOException;

public class PrimaryDatabaseReader<T>
	extends AbstractDatabaseReader<T, PrimaryConfig<T>>
	implements Closeable
	{
	public PrimaryDatabaseReader(PrimaryConfig<T> config) throws IOException
		{
		super(config);
		}
	
	
	}
