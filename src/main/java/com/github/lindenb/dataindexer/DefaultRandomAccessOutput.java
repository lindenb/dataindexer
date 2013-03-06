package com.github.lindenb.dataindexer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
/**
 * default implementation of {@link RandomAccessOutput}
 * using a {@link FileOutputStream}
 *
 */
public class DefaultRandomAccessOutput extends RandomAccessOutput
	{
	private long offset=0L;
	public DefaultRandomAccessOutput(File file) throws IOException
		{
		super(new FileOutputStream(file));
		}

	@Override
	public long getOffset()
		{
		return this.offset;
		}

	@Override
	/* all parent 'write' are final. */
	public void write(byte[] b, int off, int len) throws IOException
		{
		getDelegate().write(b, off, len);
		this.offset+=len;
		}

	}
