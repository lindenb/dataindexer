package com.github.lindenb.dataindexer;

import java.io.IOException;
import java.io.OutputStream;

public abstract class RandomAccessOutput extends OutputStream
	{
	private OutputStream delegate;
	public RandomAccessOutput(OutputStream delegate)
		{
		this.delegate=delegate;
		}

	protected OutputStream getDelegate() {
		return delegate;
		}
	
	public abstract long getOffset();
	
	@Override
	public abstract void write(byte[] b, int off, int len) throws IOException;
	
	@Override
	public final void write(byte[] b) throws IOException
		{
		this.write(b,0,b.length);
		}
	
	@Override
	public final void write(int b) throws IOException
		{
		this.write(new byte[]{(byte)b},0,1);
		}
	
	@Override
	public void flush() throws IOException
		{
		getDelegate().flush();
		}
	
	@Override
	public void close() throws IOException
		{
		flush();
		getDelegate().close();
		}
	
	}
