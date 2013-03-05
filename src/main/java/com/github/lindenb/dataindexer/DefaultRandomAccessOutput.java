package com.github.lindenb.dataindexer;

import java.io.IOException;
import java.io.OutputStream;

public class DefaultRandomAccessOutput extends RandomAccessOutput
	{
	private long offset=0L;
	public DefaultRandomAccessOutput(OutputStream delegate)
		{
		super(delegate);
		}

	@Override
	public long getOffset()
		{
		return this.offset;
		}

	@Override
	public void write(byte[] b, int off, int len) throws IOException
		{
		getDelegate().write(b, off, len);
		this.offset+=len;
		}

}
