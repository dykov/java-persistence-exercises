package com.bobocode.dao;

import com.bobocode.exception.CompanyDaoException;
import com.bobocode.model.Company;
import org.hibernate.Session;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Query;

public class CompanyDaoImpl implements CompanyDao {
    private EntityManagerFactory entityManagerFactory;

    private static final String SELECT_QUERY = """
            select c
            from Company c
            join fetch c.products
            where c.id = :id
            """;

    public CompanyDaoImpl(EntityManagerFactory entityManagerFactory) {
        this.entityManagerFactory = entityManagerFactory;
    }

    @Override
    public Company findByIdFetchProducts(Long id) {
        final EntityManager entityManager = entityManagerFactory.createEntityManager();
        entityManager.unwrap(Session.class).setDefaultReadOnly(true);
        entityManager.getTransaction().begin();
        try {
            final Query query = entityManager.createQuery(SELECT_QUERY);
            query.setParameter("id", id);
            final Company result = (Company) query.getSingleResult();
            entityManager.getTransaction().commit();
            return result;
        } catch(Exception e) {
            entityManager.getTransaction().rollback();
            throw new CompanyDaoException("Cannot perform read operation", e);
        } finally {
            entityManager.close();
        }
    }
}
