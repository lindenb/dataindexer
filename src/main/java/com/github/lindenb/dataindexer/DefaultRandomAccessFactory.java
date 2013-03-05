package com.github.lindenb.dataindexer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class DefaultRandomAccessFactory implements
		RandomAccessFactory
	{
	@Override
	public RandomAccessOutput openForWriting(File file) throws IOException
		{
		return new DefaultRandomAccessOutput(new FileOutputStream(file));
		}
	@Override
	public RandomAccessInput openForReading(File file) throws IOException {
		return new DefaultRandomAccessInput(file);
		}
	}
