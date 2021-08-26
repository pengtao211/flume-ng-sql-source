package org.keedio.flume.source;

import java.text.SimpleDateFormat;
import java.util.*;

import org.hibernate.CacheMode;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.transform.Transformers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flume.Context;

/**
 * Helper class to manage hibernate sessions and perform queries
 * 
 * @author <a href="mailto:mvalle@keedio.com">Marcelo Valle</a>
 *
 */
public class HibernateHelper {

	private static final Logger LOG = LoggerFactory
			.getLogger(HibernateHelper.class);

	private static SessionFactory factory;
	private Session session;
	private ServiceRegistry serviceRegistry;
	private Configuration config;
	private SQLSourceHelper sqlSourceHelper;
	private String maxSql;
	private String maxtime;
	private String ts;
	private SimpleDateFormat dateFormat;
	private Date date;

	/**
	 * Constructor to initialize hibernate configuration parameters
	 * @param sqlSourceHelper Contains the configuration parameters from flume config file
	 */
	public HibernateHelper(SQLSourceHelper sqlSourceHelper) {

		this.sqlSourceHelper = sqlSourceHelper;
		Context context = sqlSourceHelper.getContext();

		/* check for mandatory propertis */
		sqlSourceHelper.checkMandatoryProperties();

		Map<String,String> hibernateProperties = context.getSubProperties("hibernate.");
		Iterator<Map.Entry<String,String>> it = hibernateProperties.entrySet().iterator();
		
		config = new Configuration();
		dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Map.Entry<String, String> e;
		
		while (it.hasNext()){
			e = it.next();
			config.setProperty("hibernate." + e.getKey(), e.getValue());
		}

	}

	/**
	 * Connect to database using hibernate
	 */
	public void establishSession() {

		LOG.info("Opening hibernate session");

		serviceRegistry = new StandardServiceRegistryBuilder()
				.applySettings(config.getProperties()).build();
		factory = config.buildSessionFactory(serviceRegistry);
		session = factory.openSession();
		session.setCacheMode(CacheMode.IGNORE);
		
		session.setDefaultReadOnly(sqlSourceHelper.isReadOnlySession());
	}

	/**
	 * Close database connection
	 */
	public void closeSession() {

		LOG.info("Closing hibernate session");

		session.close();
		factory.close();
	}

	/**
	 * Execute the selection query in the database
	 * @return The query result. Each Object is a cell content. <p>
	 * The cell contents use database types (date,int,string...), 
	 * keep in mind in case of future conversions/castings.
	 * @throws InterruptedException 
	 */
	@SuppressWarnings("unchecked")
	public List<List<Object>> executeQuery() throws InterruptedException {
		
		List<List<Object>> rowsList = new ArrayList<List<Object>>() ;
		Query query;
		
		if (!session.isConnected()){
			resetConnection();
		}

		//判断是否是配置增量更新的时间字段，有的话就按照时间增量更新，否则走原来的逻辑
		if (sqlSourceHelper.isIncrementalQuery()){
			maxSql = sqlSourceHelper.maxQuery();
//			LOG.info("sql:"+maxSql);
			List<List<Object>> max = session.createSQLQuery(maxSql).setResultTransformer(Transformers.TO_LIST).list();
			maxtime = max.get(0).get(0).toString().substring(0,19);
			try {
				date = dateFormat.parse(maxtime);
				ts = String.valueOf(date.getTime() / 1000);
				query = session.createSQLQuery(sqlSourceHelper.buildQuery(ts));
				if (sqlSourceHelper.getMaxRows() != 0){
					query = query.setMaxResults(sqlSourceHelper.getMaxRows());
				}
				rowsList = query.setFetchSize(sqlSourceHelper.getMaxRows()).setResultTransformer(Transformers.TO_LIST).list();
//				LOG.info("Current time is "+sqlSourceHelper.getCurrentIndex()+",and lasttime is " + ts);
//				LOG.info("Records count: "+rowsList.size());
			}catch (Exception e){
				LOG.error("Exception thrown, resetting connection.",e);
				resetConnection();
			}
			if (!rowsList.isEmpty()){
				sqlSourceHelper.setCurrentIndex(ts);
			}
		//原有的逻辑
		} else {
			if (sqlSourceHelper.isCustomQuerySet()) {
					query = session.createSQLQuery(sqlSourceHelper.buildQuery());

				if (sqlSourceHelper.getMaxRows() != 0){
					query = query.setMaxResults(sqlSourceHelper.getMaxRows());
				}
			}
			else
			{
				query = session
						.createSQLQuery(sqlSourceHelper.getQuery())
						.setFirstResult(Integer.parseInt(sqlSourceHelper.getCurrentIndex()));

				if (sqlSourceHelper.getMaxRows() != 0){
					query = query.setMaxResults(sqlSourceHelper.getMaxRows());
				}
			}

			try {
				rowsList = query.setFetchSize(sqlSourceHelper.getMaxRows()).setResultTransformer(Transformers.TO_LIST).list();
			}catch (Exception e){
				LOG.error("Exception thrown, resetting connection.",e);
				resetConnection();
			}

			if (!rowsList.isEmpty()){
				sqlSourceHelper.setCurrentIndex(Integer.toString((Integer.parseInt(sqlSourceHelper.getCurrentIndex())
						+ rowsList.size())));
			}
		}
		
		return rowsList;
	}

	private void resetConnection() throws InterruptedException{
		if(session.isOpen()){
			session.close();
			factory.close();
		} else {
			establishSession();
		}
		
	}
}
