package com.github.lindenb.dataindexer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class DefaultRandomAccessInput extends RandomAccessInput {
	private RandomAccessFile delegate;
	public DefaultRandomAccessInput(File file) throws IOException
		{
		this.delegate=new RandomAccessFile(file, "r");
		}
	protected RandomAccessFile getDelegate() {
		return delegate;
		}
	@Override
	public void seek(long offset) throws IOException
		{
		getDelegate().seek(offset);
		}

	@Override
	public int read() throws IOException
		{
		return getDelegate().read();
		}
	
	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		return  getDelegate().read(b,off,len);
		}
	@Override
	public int read(byte[] b) throws IOException {
		return  getDelegate().read(b);
		}
	@Override
	public void close() throws IOException
		{
		getDelegate().close();
		}
}
