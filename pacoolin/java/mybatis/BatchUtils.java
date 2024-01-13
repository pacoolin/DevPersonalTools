package com.eastmoney.esrm.service.util;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.ibatis.executor.BatchResult;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.defaults.DefaultSqlSessionFactory;
import org.springframework.stereotype.Component;
import tk.mybatis.mapper.common.BaseMapper;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * 数据库批处理操作
 */
<dependency>
    <groupId>tk.mybatis</groupId>
    <artifactId>mapper-spring-boot-starter</artifactId>
    <version>1.2.0</version>
</dependency>

@Component
public class BatchUtils {

    public static <T> Integer saveBatch(List<T> datas, int cacheSize, Class<? extends BaseMapper<T>> mapperClass, SqlSessionFactory sqlSessionFactoryP) {
        int total = 0;
        if (CollectionUtils.isNotEmpty(datas)) {
            /* 开启批处理（自动提交，由spring事务控制）*/
            DefaultSqlSessionFactory sqlSessionFactory = (DefaultSqlSessionFactory) sqlSessionFactoryP;
            //获取一个批处理sqlSession
            SqlSession batchSqlSession = sqlSessionFactory.openSession(ExecutorType.BATCH,true);
            int count = 0;
            BaseMapper<T> mapper = batchSqlSession.getMapper(mapperClass);
            try{
                for (T data : datas) {
                    mapper.insert(data);
                    if ((count + 1) % cacheSize == 0) {
                        total += flushStatement(batchSqlSession);
                    }
                    count++;
                }
                total += flushStatement(batchSqlSession);
            } finally {
                batchSqlSession.close();
            }
        }
        return total;
    }

    public static Integer updateBatch(List datas, int cacheSize, Class<? extends BaseMapper> mapperClass, SqlSessionFactory sqlSessionFactoryP) {
        int total = 0;
        if (CollectionUtils.isNotEmpty(datas)) {
            /* 开启批处理（自动提交，由spring事务控制）*/
            DefaultSqlSessionFactory sqlSessionFactory = (DefaultSqlSessionFactory) sqlSessionFactoryP;
            //获取一个批处理sqlSession
            SqlSession batchSqlSession = sqlSessionFactory.openSession(ExecutorType.BATCH,true);
            int count = 0;
            BaseMapper mapper = batchSqlSession.getMapper(mapperClass);
            try{
                for (Object data : datas) {
                    mapper.updateByPrimaryKey(data);
                    if ((count + 1) % cacheSize == 0) {
                        total += flushStatement(batchSqlSession);
                    }
                    count++;
                }
                total += flushStatement(batchSqlSession);
            } finally {
                batchSqlSession.close();
            }
        }
        return total;
    }

    public static <T,V> Integer executeBatch(List<V> datas, int cacheSize, Class<T> mapperClass, SqlSessionFactory sqlSessionFactoryP,Function<T,Consumer<V>> mapperMethodFunction) {
        int total = 0;
        if (CollectionUtils.isNotEmpty(datas)) {
            /* 开启批处理（自动提交，由spring事务控制）*/
            DefaultSqlSessionFactory sqlSessionFactory = (DefaultSqlSessionFactory) sqlSessionFactoryP;
            //获取一个批处理sqlSession
            SqlSession batchSqlSession = sqlSessionFactory.openSession(ExecutorType.BATCH,true);
            int count = 0;
            T mapper = batchSqlSession.getMapper(mapperClass);
            try{
                for (V data : datas) {
                    mapperMethodFunction.apply(mapper).accept(data);
                    if ((count + 1) % cacheSize == 0) {
                        total += flushStatement(batchSqlSession);
                    }
                    count++;
                }
                total += flushStatement(batchSqlSession);
            } finally {
                batchSqlSession.close();
            }
        }
        return total;
    }
    public static int flushStatement(SqlSession batchSqlSession) {
        List<BatchResult> batchResults = batchSqlSession.flushStatements();
        if (CollectionUtils.isNotEmpty(batchResults)) {
            return batchResults.stream().mapToInt(s -> s.getUpdateCounts().length).sum();
        }
        return 0;
    }

    public static <T> void batchProcessLqRecords(List<T> list, int batchSize, Consumer<List<T>> eachConsumer){
        if(CollectionUtils.isNotEmpty(list)){
            int size = list.size();
            int count = size/batchSize+1;
            for(int i = 0;i<count;i++){
                int thisStart = batchSize * i;
                int nextStart = (i + 1) * batchSize;
                if(size == thisStart){
                    break;
                }
                int toIndex = Math.min(size, nextStart);
                List<T> eachList = list.subList(thisStart,toIndex);
                if(CollectionUtils.isNotEmpty(eachList)){
                    eachConsumer.accept(eachList);
                }
            }
        }
    }
}
