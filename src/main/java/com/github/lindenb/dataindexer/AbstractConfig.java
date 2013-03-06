package com.github.lindenb.dataindexer;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class AbstractConfig<T> 
	{
	private RandomAccessFactory offsetStreamFactory=null;
	private File homeDir;
	private String name;
	private Integer sizeof_item;

	private Logger logger;

	public File getIndexFile()
		{
		return new File(this.getHomeDirectory(),getName()+".idx"); 
		}
	public File getHomeDirectory()
		{
		return homeDir;
		}
	
	
	public void setHomeDirectory(File homeDir)
		{
		this.homeDir = homeDir;
		}
	
	public String getName()
		{
		return name;
		}
	
	public void setName(String name)
		{
		this.name = name;
		}

	public File getDataFile()
		{
		return new File(this.getHomeDirectory(),getName()+".dat"); 
		}
	
	public File getSummaryFile()
		{
		return new File(this.getHomeDirectory(),getName()+".def"); 
		}

	public Integer getSizeOfItem()
		{
		return sizeof_item;
		}
	
	public void setSizeOfItem(Integer sizeof_item)
		{
		this.sizeof_item = sizeof_item;
		}
	
	public boolean isFixedSizeof()
		{
		return getSizeOfItem()!=null;
		}
	
	public void setRandomAccessFactory(
			RandomAccessFactory offsetOutputStreamFactory)
		{
		this.offsetStreamFactory = offsetOutputStreamFactory;
		}

	
	public RandomAccessFactory getRandomAccessFactory()
		{
		if(offsetStreamFactory==null)
			{
			offsetStreamFactory=new DefaultRandomAccessFactory();
			}
		return offsetStreamFactory;
		}
	
	public abstract TupleBinding<T> getDataBinding();
	
	public void setLogger(Logger logger)
		{
		this.logger = logger;
		}
	
	public Logger getLogger()
		{
		if(logger==null)
			{
			logger=Logger.getLogger("log"+getName());
			logger.setLevel(Level.OFF);
			}
		return logger;
		}

	
	}
