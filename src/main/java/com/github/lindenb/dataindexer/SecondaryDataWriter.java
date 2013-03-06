package com.github.lindenb.dataindexer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class SecondaryDataWriter<PRIMARY,K>
	extends AbstractDataIndexer<ObjectAndOffset<K>,SecondaryConfig<PRIMARY,K>>
	{
	protected DataOutputStream tmpOut;
	private PrimaryDataIndexWriter<PRIMARY> owner;
	private File tmpFile1;
	
	
	private Comparator<ObjectAndOffset<K>> objectAndOffsetComparator=null;
	
	void setOwner(PrimaryDataIndexWriter<PRIMARY> owner)
		{
		this.owner=owner;
		}
	
	public PrimaryDataIndexWriter<PRIMARY> getOwner() {
		return owner;
		}
	
	public SecondaryDataWriter(SecondaryConfig<PRIMARY,K> config) throws IOException
		{
		super(config);
		this.objectAndOffsetComparator=config.createObjectAndOffsetComparator();
		}
	
	
	private void ensureOpen()throws IOException
		{
		if(this.tmpOut==null)
			{
			this.tmpFile1=File.createTempFile("tmp.",".data",getOwner().getConfig().getHomeDirectory());
			FileOutputStream fos=new FileOutputStream(tmpFile1);
			this.tmpOut=new DataOutputStream(fos);
			}
		}
		
	public void put(PRIMARY object,long primaryoffset) throws IOException
		{
		ensureOpen();
		for(K k:getConfig().getKeyCreator().getSecondaryKeys(object))
			{
			ObjectAndOffset<K> oao=new ObjectAndOffset<K>(k, primaryoffset);
			getDataBinding().writeObject(oao, this.tmpOut);
			++numberOfItems;
			}
		}
	
	
	
	@Override
	public void close() throws IOException
		{
		tmpOut.flush();
		tmpOut.close();
		externalSort();
		/*
		FileInputStream dataIn=new FileInputStream("x");
		List<ObjectAndOffset> buffer=new ArrayList<ObjectAndOffset>();
		int index_buffer=0;
		long indexFile=0L;
		File tmpFile2=null;
		while(indexFile<this.numberOfItems)
			{
			//fill buffer
			while(buffer.size()<10000 && indexFile< this.numberOfItems)
				{
				buffer.add(objectAndOffsetBinding.readObject(dataIn));
				indexFile++;
				}
			//sort the buffer
			Collections.sort(buffer,objectAndOffsetComparator);
			//no previous file
			if(tmpFile2==null)
				{
				//dump everty think
				tmpFile2=new File("TODO2");
				FileOutputStream fout=new FileOutputStream(tmpFile2);
				for(ObjectAndOffset o:buffer)
					{
					objectAndOffsetBinding.writeObject(o, fout);
					}
				fout.flush();
				fout.close();
				buffer.clear();
				}
			else //merge sort with previous file
				{
				ObjectAndOffset curr=null;
				boolean need_read_from_file=true;
				while(!buffer.isEmpty() && count_file>0)
					{
					
					}
				
				}
			}
			*/
		}
	
	 private File tmpFile() throws IOException
	 	{
		return File.createTempFile("_tmp.",".data",
				getOwner().getConfig().getHomeDirectory()
				);
	 	} 
	
	 private class FileAndSize
	 	{
		File file;
		long count=0L;
		DataInputStream in;
		DataOutputStream out;
		void openRead() throws IOException
			{
			in=new DataInputStream(new FileInputStream(this.file));
			}
		void openWrite() throws IOException
			{
			out=new DataOutputStream(new FileOutputStream(this.file));
			}
		
		ObjectAndOffset<K> read()  throws IOException
			{
			if(count<=0) throw new IOException("empty set");
			ObjectAndOffset<K> oao=getDataBinding().readObject(this.in);
			count--;
			return oao;
			}
		
		void write(ObjectAndOffset<K> oao) throws IOException
			{
			getDataBinding().writeObject(oao,this.out);
			++count;
			}
		
		void close() throws IOException
			{
			if(in!=null) in.close();
			if(out!=null)
				{
				out.flush();
				out.close();
				}
			}	
	 	}
	 
	 private void externalSort() throws IOException
		 {
		 int buffer_capacity=100000;
		 FileAndSize prevFile=null;
		 List<ObjectAndOffset<K>> buffer = new ArrayList<ObjectAndOffset<K>>(buffer_capacity);
		
		 FileAndSize rootFile=new FileAndSize();
		 rootFile.count=this.numberOfItems;
		 rootFile.file=this.tmpFile1;
		 rootFile.openRead();
		 
		 // Iterate through the elements in the file
		 while(rootFile.count>0)
		 	{
			 buffer.clear();
			 // Read M-element chunk at a time from the file
			 while(rootFile.count>0 && buffer.size()<buffer_capacity)
				 {
				 ObjectAndOffset<K> oao=rootFile.read();
				 buffer.add(oao);
				 }
			// Sort M elements
			 Collections.sort(buffer,this.objectAndOffsetComparator);
			 if(prevFile==null)
			 	{
				 prevFile=new FileAndSize();
				 prevFile.file=tmpFile();
				 prevFile.openWrite();
				 for (ObjectAndOffset<K> oao:buffer)
				 	{
					prevFile.write(oao);
				 	}
				prevFile.close();
			 	}
			 else
			 	{
				FileAndSize nextFile=new FileAndSize();
				nextFile.file=tmpFile();
				nextFile.openWrite();
				
				ObjectAndOffset<K> diskItem=null;
				ObjectAndOffset<K> objectItem=null;
				Iterator<ObjectAndOffset<K>> iter=buffer.iterator();
				
				System.err.println("Merging "+nextFile.file+"/"+prevFile.file);
				prevFile.openRead();
				
				
				for(;;)
					{
					if(objectItem==null && !iter.hasNext()) break;
					if(diskItem==null && prevFile.count<=0) break;
					if(objectItem==null)
						{
						objectItem=iter.next();
						}
					if(diskItem==null)
						{
						diskItem=prevFile.read();
						}
					if(this.objectAndOffsetComparator.compare(objectItem, diskItem)<=0)
						{
						nextFile.write(objectItem);
						objectItem=null;
						}
					else
						{
						nextFile.write(diskItem);
						diskItem=null;
						}
					}
				if(objectItem!=null)
					{
					if(diskItem!=null) throw new IllegalStateException();
					nextFile.write(objectItem);
					}
				while(iter.hasNext())
					{
					objectItem=iter.next();
					nextFile.write(objectItem);
					}
				if(diskItem!=null)
					{
					if(objectItem!=null) throw new IllegalStateException();
					nextFile.write(diskItem);
					}
				while(prevFile.count>0)
					{
					diskItem=prevFile.read();
					nextFile.write(diskItem);
					}
					
				prevFile.close();
				nextFile.close();
				
				prevFile.file.delete();
				prevFile=nextFile;
			 	}
			 }
		rootFile.close();
	
		
		if(prevFile.count!=this.numberOfItems) throw new IOException();	
		prevFile.openRead();
		
		RandomAccessOutput dataOut=getConfig().getRandomAccessFactory().openForWriting(
				getConfig().getDataFile()
				);
		RandomAccessFile indexFile=null;
		if(!getConfig().isFixedSizeof())
			{
			indexFile=new RandomAccessFile(getConfig().getIndexFile(), "rw");
			}
		
		for(long n=0;n< prevFile.count;++n)
			{
			ObjectAndOffset<K> o=prevFile.read();
			long offset=-1L;
			
			
			if(indexFile!=null)
				{
				offset=dataOut.getOffset();
				indexFile.writeLong(offset);
				}
			
			DataOutputStream daos=new DataOutputStream(dataOut);
			getDataBinding().writeObject(o,daos);
			daos.flush();
			}
		
		if(indexFile!=null)
			{
			indexFile.close();
			}
		
		dataOut.flush();
		dataOut.close();
		
		prevFile.close();
		prevFile.file.delete();
		this.tmpFile1.delete();
		
		}
			 
		 
		 
		
		
		 
	
	
	}
