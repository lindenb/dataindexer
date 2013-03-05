package com.github.lindenb.dataindexer;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PrimaryConfig<T> extends AbstractConfig
	{
	private File homeDir;
	private String name;
	private RandomAccessFactory offsetStreamFactory=null;
	private TupleBinding<T> dataBinding;

	private Logger logger;
	public File getHomeDirectory()
		{
		return homeDir;
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
	
	
	public File getIndexFile()
		{
		return new File(this.getHomeDirectory(),getName()+".idx"); 
		}

	public File getDataFile()
		{
		return new File(this.getHomeDirectory(),getName()+".dat"); 
		}

	public File getSummaryFile()
		{
		return new File(this.getHomeDirectory(),getName()+".def"); 
		}

	
	public void setDataBinding(TupleBinding<T> dataBinding)
		{
		this.dataBinding = dataBinding;
		}
	
	public TupleBinding<T> getDataBinding() {
		return dataBinding;
		}
	
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
