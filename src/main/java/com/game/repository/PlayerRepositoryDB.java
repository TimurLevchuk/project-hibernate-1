package com.game.repository;

import com.game.entity.Player;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.query.NativeQuery;
import org.hibernate.query.Query;
import org.springframework.stereotype.Repository;

import javax.annotation.PreDestroy;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;

@Repository(value = "db")
public class PlayerRepositoryDB implements IPlayerRepository {

    private final SessionFactory sessionFactory;

    public PlayerRepositoryDB() {
        Properties properties = new Properties();
        properties.put(Environment.DIALECT, "org.hibernate.dialect.MySQL8Dialect");
        properties.put(Environment.DRIVER, "com.p6spy.engine.spy.P6SpyDriver");
        properties.put(Environment.URL, "jdbc:p6spy:mysql://localhost:3306/rpg");
        properties.put(Environment.USER, "root");
        properties.put(Environment.PASS, "mysql");
        properties.put(Environment.HBM2DDL_AUTO, "update");

        sessionFactory = new Configuration()
                .addAnnotatedClass(Player.class)
                .addProperties(properties)
                .buildSessionFactory();
    }

    @Override
    public List<Player> getAll(int pageNumber, int pageSize) {
        return executeInSession(session -> {
            NativeQuery<Player> nativeQuery = session.createNativeQuery("SELECT * FROM rpg.player", Player.class);
            nativeQuery.setFirstResult(pageNumber * pageSize);
            nativeQuery.setMaxResults(pageSize);
            return nativeQuery.list();
        });
    }

    @Override
    public int getAllCount() {
        return executeInSession(session -> {
            Query<Long> query = session.createNamedQuery("player_getAllCount", Long.class);
            return Math.toIntExact(query.uniqueResult());
        });
    }

    @Override
    public Player save(Player player) {
        return executeInTransaction(session -> {
            session.save(player);
            return player;
        });
    }

    @Override
    public Player update(Player player) {
        return executeInTransaction(session -> {
            session.update(player);
            return player;
        });
    }

    @Override
    public Optional<Player> findById(long id) {
        return executeInSession(session -> Optional.ofNullable(session.find(Player.class, id)));
    }

    @Override
    public void delete(Player player) {
        executeInTransaction(session -> {
            session.remove(player);
            return null;
        });
    }

    @PreDestroy
    public void beforeStop() {
        sessionFactory.close();
    }

    private <R> R executeInSession(Function<Session, R> function) {
        try (Session session = sessionFactory.openSession()) {
            return function.apply(session);
        }
    }

    private <R> R executeInTransaction(Function<Session, R> function) {
        try (Session session = sessionFactory.openSession()) {
            Transaction transaction = session.beginTransaction();
            try {
                R result = function.apply(session);
                transaction.commit();
                return result;
            } catch (Exception e) {
                transaction.rollback();
                throw e;
            }
        }
    }
}