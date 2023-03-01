package com.bobocode.dao;

import com.bobocode.exception.AccountDaoException;
import com.bobocode.model.Account;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.TypedQuery;
import java.util.List;

public class AccountDaoImpl implements AccountDao {
    private EntityManagerFactory emf;

    public AccountDaoImpl(EntityManagerFactory emf) {
        this.emf = emf;
    }

    @Override
    public void save(Account account) {
        final EntityManager entityManager = emf.createEntityManager();
        entityManager.getTransaction().begin();
        try {
            entityManager.persist(account);
            entityManager.getTransaction().commit();
        } catch(Exception e) {
            entityManager.getTransaction().rollback();
            throw new AccountDaoException("Cannot save", e);
        } finally {
            entityManager.close();
        }
    }

    @Override
    public Account findById(Long id) {
        final EntityManager entityManager = emf.createEntityManager();
        entityManager.getTransaction().begin();
        try {
            final Account account = entityManager.find(Account.class, id);
            entityManager.getTransaction().commit();
            return account;
        } catch(Exception e) {
            entityManager.getTransaction().rollback();
            throw new AccountDaoException("Cannot find by id", e);
        } finally {
            entityManager.close();
        }
    }

    @Override
    public Account findByEmail(String email) {
        final EntityManager entityManager = emf.createEntityManager();
        entityManager.getTransaction().begin();
        try {
            final TypedQuery<Account> namedQuery = entityManager.createQuery(
                    "SELECT a FROM Account a WHERE a.email = :email",
                    Account.class
            );
            namedQuery.setParameter("email", email);
            final Account account = namedQuery.getSingleResult();
            entityManager.getTransaction().commit();
            return account;
        } catch(Exception e) {
            entityManager.getTransaction().rollback();
            throw new AccountDaoException("Cannot find by email", e);
        } finally {
            entityManager.close();
        }
    }

    @Override
    public List<Account> findAll() {
        final EntityManager entityManager = emf.createEntityManager();
        entityManager.getTransaction().begin();
        try {
            final TypedQuery<Account> query = entityManager.createQuery(
                    "SELECT a FROM Account a",
                    Account.class
            );
            final List<Account> accounts = query.getResultList();
            entityManager.getTransaction().commit();
            return accounts;
        } catch(Exception e) {
            entityManager.getTransaction().rollback();
            throw new AccountDaoException("Cannot find all", e);
        } finally {
            entityManager.close();
        }
    }

    @Override
    public void update(Account account) {
        final EntityManager entityManager = emf.createEntityManager();
        entityManager.getTransaction().begin();
        try {
            entityManager.merge(account);
            entityManager.getTransaction().commit();
        } catch(Exception e) {
            entityManager.getTransaction().rollback();
            throw new AccountDaoException("Cannot update", e);
        } finally {
            entityManager.close();
        }
    }

    @Override
    public void remove(Account account) {
        final EntityManager entityManager = emf.createEntityManager();
        entityManager.getTransaction().begin();
        try {
            final Account merge = entityManager.merge(account);
            entityManager.remove(merge);
            entityManager.getTransaction().commit();
        } catch(Exception e) {
            entityManager.getTransaction().rollback();
            throw new AccountDaoException("Cannot remove", e);
        } finally {
            entityManager.close();
        }
    }
}

