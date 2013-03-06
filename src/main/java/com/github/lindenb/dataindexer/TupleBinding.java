package com.github.lindenb.dataindexer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public interface TupleBinding<T>
	{
	public void writeObject(final T o,DataOutputStream out) throws IOException;
	public T readObject(DataInputStream in) throws IOException;
	}
