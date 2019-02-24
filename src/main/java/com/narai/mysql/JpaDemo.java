package com.narai.mysql;

import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import javax.persistence.EntityManager;
import javax.persistence.FlushModeType;
import javax.persistence.Query;
import java.util.List;

/**
 * @author: kaze
 * @date: 2019-02-24
 */
@Component
public class JpaDemo {

    @Resource
    private EntityManager entityManager;

    public void study() {
        Object object = new Object();
        {
            // insert
            entityManager.persist(object);
        }
        {
            // update
            // 被容器管理的话，直接set对象属性值即可，想要立马起效则
            entityManager.flush();
            // 未被容器管理
            entityManager.merge(object);
        }
        {
            // delete
            entityManager.remove(object);
        }
        {
            // find
            // 根据主键
            entityManager.find(Object.class, 1);

            // hql
            Query query = entityManager.createQuery("select o from Object o where o.id =?1");
            query.setParameter(1, 1);
            List list = query.getResultList();

            // sql
            query = entityManager.createNativeQuery("select * from object", Object.class);
            list = query.getResultList();
        }
        {
            // 所有被管理的实体都会从持久状态分离出来
            entityManager.clear();
        }
        {
            // 默认
            entityManager.setFlushMode(FlushModeType.AUTO);
            // 手动
            entityManager.setFlushMode(FlushModeType.COMMIT);
            // 是否开启实体管理
            entityManager.isOpen();
            // 获取事务
            entityManager.getTransaction();
        }
    }

}
