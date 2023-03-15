package com.bobocode.dao;

import com.bobocode.model.Photo;
import com.bobocode.model.PhotoComment;
import com.bobocode.util.ExerciseNotCompletedException;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Please note that you should not use auto-commit mode for your implementation.
 */
public class PhotoDaoImpl implements PhotoDao {
    private final EntityManagerFactory entityManagerFactory;

    public PhotoDaoImpl(EntityManagerFactory entityManagerFactory) {
        this.entityManagerFactory = entityManagerFactory;
    }

    @Override
    public void save(Photo photo) {
        performInTransaction(
                entityManager -> entityManager.persist(photo)
        );
    }

    @Override
    public Photo findById(long id) {
        return performAndReturnInTransaction(
                entityManager -> entityManager.find(Photo.class, id)
        );
    }

    @Override
    public List<Photo> findAll() {
        return performAndReturnInTransaction(
                entityManager -> entityManager.createQuery("select p from Photo p").getResultList()
        );
    }

    @Override
    public void remove(Photo photo) {
        performInTransaction(
                entityManager -> {
                    final Photo merged = entityManager.merge(photo);
                    entityManager.remove(merged);
                }
        );
    }

    @Override
    public void addComment(long photoId, String comment) {
        performInTransaction(
                entityManager -> {
                    final Photo photo = entityManager.getReference(Photo.class, photoId);
                    PhotoComment photoComment = new PhotoComment(comment, photo);
                    entityManager.persist(photoComment);
                }
        );
    }


    private void performInTransaction(final Consumer<EntityManager> entityManagerConsumer) {
        final EntityManager entityManager = entityManagerFactory.createEntityManager();
        entityManager.getTransaction().begin();
        try {
            entityManagerConsumer.accept(entityManager);
            entityManager.getTransaction().commit();
        } catch(Exception e) {
            entityManager.getTransaction().rollback();
            throw e;
        } finally {
            entityManager.close();
        }
    }

    private <T> T performAndReturnInTransaction(final Function<EntityManager, T> entityManagerTFunction) {
        final EntityManager entityManager = entityManagerFactory.createEntityManager();
        entityManager.getTransaction().begin();
        try {
            final T result = entityManagerTFunction.apply(entityManager);
            entityManager.getTransaction().commit();
            return result;
        } catch(Exception e) {
            entityManager.getTransaction().rollback();
            throw e;
        } finally {
            entityManager.close();
        }
    }

}
