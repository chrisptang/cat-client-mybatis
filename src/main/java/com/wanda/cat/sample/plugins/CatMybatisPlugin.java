package com.wanda.cat.sample.plugins;

import com.dianping.cat.Cat;
import com.dianping.cat.message.Message;
import com.dianping.cat.message.Transaction;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.plugin.*;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.type.TypeHandlerRegistry;
import org.mybatis.spring.transaction.SpringManagedTransaction;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StopWatch;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.text.DateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;

/**
 * 对MyBatis进行拦截，添加Cat监控
 *
 * @author Steven
 * @updated_by ptang@leqee.com @ 2020-09-30
 */
@Intercepts({
        @Signature(method = "query", type = Executor.class, args = {
                MappedStatement.class, Object.class, RowBounds.class,
                ResultHandler.class}),
        @Signature(method = "update", type = Executor.class, args = {MappedStatement.class, Object.class})
})
public class CatMybatisPlugin implements Interceptor {

    private Properties properties;

    private volatile long slowSqlMillis = 10_000L; //default to be 10 seconds.

    private static Log logger = LogFactory.getLog(CatMybatisPlugin.class);

    //缓存，提高性能
    private static final Map<String, String> SQL_URL_CACHE = new ConcurrentHashMap<String, String>(256);

    private static final String EMPTY_CONNECTION = "jdbc:mysql://unknown:3306/%s?useUnicode=true";

    private Executor target;

    // druid 数据源的类名称
    private static final String DRUID_DATA_SOURCE_CLASS_NAME = "com.alibaba.druid.pool.DruidDataSource";
    // dbcp 数据源的类名称
    private static final String DBCP_BASIC_DATA_SOURCE_CLASS_NAME = "org.apache.commons.dbcp.BasicDataSource";
    // dbcp2 数据源的类名称
    private static final String DBCP_2_BASIC_DATA_SOURCE_CLASS_NAME = "org.apache.commons.dbcp2.BasicDataSource";
    // c3p0 数据源的类名称
    private static final String C_3_P_0_COMBO_POOLED_DATA_SOURCE_CLASS_NAME = "com.mchange.v2.c3p0.ComboPooledDataSource";
    // HikariCP 数据源的类名称
    private static final String HIKARI_CP_DATA_SOURCE_CLASS_NAME = "com.zaxxer.hikari.HikariDataSource";
    // BoneCP 数据源的类名称
    private static final String BONE_CP_DATA_SOURCE_CLASS_NAME = "com.jolbox.bonecp.BoneCPDataSource";

    @Override
    public Object intercept(Invocation invocation) throws Throwable {
        MappedStatement mappedStatement = (MappedStatement) invocation.getArgs()[0];
        //得到类名，方法
        String[] strArr = mappedStatement.getId().split("\\.");
        String methodName = strArr[strArr.length - 2] + "." + strArr[strArr.length - 1];

        Transaction t = Cat.newTransaction("SQL", methodName);

        //得到sql语句
        Object parameter = null;
        if (invocation.getArgs().length > 1) {
            parameter = invocation.getArgs()[1];
        }
        BoundSql boundSql = mappedStatement.getBoundSql(parameter);
        Configuration configuration = mappedStatement.getConfiguration();
        String sql = showSql(configuration, boundSql);

        //获取SQL类型
        SqlCommandType sqlCommandType = mappedStatement.getSqlCommandType();
        Cat.logEvent("SQL.Method", sqlCommandType.name().toLowerCase(), Message.SUCCESS, sql);

        String sqlDatabase = this.getSQLDatabase();
        Cat.logEvent("SQL.Database", sqlDatabase);

        Object returnObj = null;
        StopWatch stopWatch = new StopWatch();
        try {
            stopWatch.start();
            returnObj = invocation.proceed();
            stopWatch.stop();
            t.setStatus(Transaction.SUCCESS);

            // 统计慢SQL；
            if (stopWatch.getLastTaskTimeMillis() >= slowSqlMillis) {
                Cat.logEvent("SQL.SlowSQL", methodName, Message.SUCCESS, sql);
            }
        } finally {
            t.complete();
        }

        return returnObj;
    }

    private javax.sql.DataSource getDataSource() {
        org.apache.ibatis.transaction.Transaction transaction = this.target.getTransaction();
        if (transaction == null) {
            logger.error(String.format("Could not find transaction on target [%s]", this.target));
            return null;
        }
        if (transaction instanceof SpringManagedTransaction) {
            String fieldName = "dataSource";
            Field field = ReflectionUtils.findField(transaction.getClass(), fieldName, javax.sql.DataSource.class);

            if (field == null) {
                logger.error(String.format("Could not find field [%s] of type [%s] on target [%s]",
                        fieldName, javax.sql.DataSource.class, this.target));
                return null;
            }

            ReflectionUtils.makeAccessible(field);
            javax.sql.DataSource dataSource = (javax.sql.DataSource) ReflectionUtils.getField(field, transaction);
            return dataSource;
        }

        logger.error(String.format("---the transaction is not SpringManagedTransaction:%s", transaction.getClass().toString()));

        return null;
    }

    /**
     * 重写 getSqlURL 方法
     *
     * @author fanlychie (https://github.com/fanlychie)
     */
    private String getSqlURL() {
        // 客户端使用的数据源
        javax.sql.DataSource dataSource = this.getDataSource();
        if (dataSource != null) {
            // 处理常见的数据源
            switch (dataSource.getClass().getName()) {
                // druid
                case DRUID_DATA_SOURCE_CLASS_NAME:
                    return getDataSourceSqlURL(dataSource, DRUID_DATA_SOURCE_CLASS_NAME, "getUrl");
                // dbcp
                case DBCP_BASIC_DATA_SOURCE_CLASS_NAME:
                    return getDataSourceSqlURL(dataSource, DBCP_BASIC_DATA_SOURCE_CLASS_NAME, "getUrl");
                // dbcp2
                case DBCP_2_BASIC_DATA_SOURCE_CLASS_NAME:
                    return getDataSourceSqlURL(dataSource, DBCP_2_BASIC_DATA_SOURCE_CLASS_NAME, "getUrl");
                // c3p0
                case C_3_P_0_COMBO_POOLED_DATA_SOURCE_CLASS_NAME:
                    return getDataSourceSqlURL(dataSource, C_3_P_0_COMBO_POOLED_DATA_SOURCE_CLASS_NAME, "getJdbcUrl");
                // HikariCP
                case HIKARI_CP_DATA_SOURCE_CLASS_NAME:
                    return getDataSourceSqlURL(dataSource, HIKARI_CP_DATA_SOURCE_CLASS_NAME, "getJdbcUrl");
                // BoneCP
                case BONE_CP_DATA_SOURCE_CLASS_NAME:
                    return getDataSourceSqlURL(dataSource, BONE_CP_DATA_SOURCE_CLASS_NAME, "getJdbcUrl");
            }
        }
        return null;
    }

    /**
     * 获取数据源的SQL地址
     *
     * @param dataSource                 数据源
     * @param runtimeDataSourceClassName 运行时真实的数据源的类名称
     * @param sqlURLMethodName           获取SQL地址的方法名称
     * @author fanlychie (https://github.com/fanlychie)
     */
    private String getDataSourceSqlURL(DataSource dataSource, String runtimeDataSourceClassName, String sqlURLMethodName) {
        Class<?> dataSourceClass = null;
        try {
            dataSourceClass = Class.forName(runtimeDataSourceClassName);
        } catch (ClassNotFoundException e) {
        }
        Method sqlURLMethod = ReflectionUtils.findMethod(dataSourceClass, sqlURLMethodName);
        return (String) ReflectionUtils.invokeMethod(sqlURLMethod, dataSource);
    }

    private String getSQLDatabase() {
//        String dbName = RouteDataSourceContext.getRouteKey();
        String dbName = null; //根据设置的多数据源修改此处,获取dbname
        if (dbName == null) {
            dbName = "DEFAULT";
        }
        String url = CatMybatisPlugin.SQL_URL_CACHE.get(dbName);
        if (url != null) {
            return url;
        }

        url = this.getSqlURL();//目前监控只支持mysql ,其余数据库需要各自修改监控服务端
        if (url == null) {
            url = String.format(EMPTY_CONNECTION, dbName);
        }
        CatMybatisPlugin.SQL_URL_CACHE.put(dbName, url);
        return url;
    }

    /**
     * 解析sql语句
     *
     * @param configuration
     * @param boundSql
     * @return
     */
    public String showSql(Configuration configuration, BoundSql boundSql) {
        Object parameterObject = boundSql.getParameterObject();
        List<ParameterMapping> parameterMappings = boundSql.getParameterMappings();
        String sql = boundSql.getSql().replaceAll("[\\s]+", " ");
        if (parameterMappings.size() > 0 && parameterObject != null) {
            TypeHandlerRegistry typeHandlerRegistry = configuration.getTypeHandlerRegistry();
            if (typeHandlerRegistry.hasTypeHandler(parameterObject.getClass())) {
                sql = sql.replaceFirst("\\?", Matcher.quoteReplacement(getParameterValue(parameterObject)));

            } else {
                MetaObject metaObject = configuration.newMetaObject(parameterObject);
                for (ParameterMapping parameterMapping : parameterMappings) {
                    String propertyName = parameterMapping.getProperty();
                    if (metaObject.hasGetter(propertyName)) {
                        Object obj = metaObject.getValue(propertyName);
                        sql = sql.replaceFirst("\\?", Matcher.quoteReplacement(getParameterValue(obj)));
                    } else if (boundSql.hasAdditionalParameter(propertyName)) {
                        Object obj = boundSql.getAdditionalParameter(propertyName);
                        sql = sql.replaceFirst("\\?", Matcher.quoteReplacement(getParameterValue(obj)));
                    }
                }
            }
        }
        return sql;
    }

    /**
     * 参数解析
     *
     * @param obj
     * @return
     */
    private String getParameterValue(Object obj) {
        String value = null;
        if (obj instanceof String) {
            value = "'" + obj.toString() + "'";
        } else if (obj instanceof Date) {
            DateFormat formatter = DateFormat.getDateTimeInstance(DateFormat.DEFAULT, DateFormat.DEFAULT, Locale.CHINA);
            value = "'" + formatter.format(new Date()) + "'";
        } else {
            if (obj != null) {
                value = obj.toString();
            } else {
                value = "";
            }

        }
        return value;
    }

    @Override
    public Object plugin(Object target) {
        if (target instanceof Executor) {
            this.target = (Executor) target;
            return Plugin.wrap(target, this);
        }
        return target;
    }

    @Override
    public void setProperties(Properties properties) {
        this.properties = properties;
        if (this.properties.containsKey("slowSqlMillis")) {
            this.slowSqlMillis = Long.parseLong(this.properties.getProperty("slowSqlMillis", "10000"));
        }
    }
}