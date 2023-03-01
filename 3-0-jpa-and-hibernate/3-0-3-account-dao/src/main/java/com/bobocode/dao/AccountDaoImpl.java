package com.bobocode.dao;

import com.bobocode.exception.AccountDaoException;
import com.bobocode.model.Account;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.TypedQuery;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

public class AccountDaoImpl implements AccountDao {
    private EntityManagerFactory emf;

    public AccountDaoImpl(EntityManagerFactory emf) {
        this.emf = emf;
    }

    @Override
    public void save(Account account) {
        this.performWithinPersistenceContext(entityManager -> entityManager.persist(account));
    }

    @Override
    public Account findById(Long id) {
        return performReturningWithinPersistenceContext(entityManager -> entityManager.find(Account.class, id));
    }

    @Override
    public Account findByEmail(String email) {
        return performReturningWithinPersistenceContext(entityManager -> {
            TypedQuery<Account> findByEmailQuery = entityManager.createQuery(
                    "SELECT acc FROM Account acc WHERE acc.email = :email",
                    Account.class
            );
            findByEmailQuery.setParameter("email", email);
            return findByEmailQuery.getSingleResult();
        });
    }

    @Override
    public List<Account> findAll() {
        return this.performReturningWithinPersistenceContext(entityManager ->
                entityManager.createQuery("SELECT acc FROM Account acc", Account.class).getResultList()
        );
    }

    @Override
    public void update(Account account) {
        performWithinPersistenceContext(entityManager -> entityManager.merge(account));
    }

    @Override
    public void remove(Account account) {
        performWithinPersistenceContext(entityManager -> {
            final Account merged = entityManager.merge(account);
            entityManager.remove(merged);
        });
    }

    private void performWithinPersistenceContext(final Consumer<EntityManager> entityManagerConsumer) {
        performReturningWithinPersistenceContext(entityManager -> {
           entityManagerConsumer.accept(entityManager);
            return null;
        });
    }

    private <T> T performReturningWithinPersistenceContext(final Function<EntityManager, T> entityManagerFunction) {
        final EntityManager entityManager = emf.createEntityManager();
        entityManager.getTransaction().begin();
        T result;
        try {
            result = entityManagerFunction.apply(entityManager);
            entityManager.getTransaction().commit();
        } catch(Exception e) {
            entityManager.getTransaction().rollback();
            throw new AccountDaoException("Cannot perform transaction", e);
        } finally {
            entityManager.close();
        }
        return result;
    }

}

