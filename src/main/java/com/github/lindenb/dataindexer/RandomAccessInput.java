package com.github.lindenb.dataindexer;

import java.io.IOException;
import java.io.InputStream;


public abstract class RandomAccessInput extends InputStream
	{
	public abstract void seek(long offset) throws IOException;
	}
