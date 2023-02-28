package com.bobocode.dao;

import com.bobocode.exception.DaoOperationException;
import com.bobocode.model.Product;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;

public class ProductDaoImpl implements ProductDao {

    private static final String SQL_INSERT = """
            INSERT INTO products (name, producer, price, expiration_date) VALUES (?,?,?,?);
            """;

    private static final String SQL_SELECT_ALL = "SELECT * FROM products" ;
    private static final String SQL_FIND_BY_ID = "SELECT * FROM products WHERE id=?" ;

    private static final String SQL_UPDATE = """
            UPDATE products
            SET
            name = ?,
            producer = ?,
            price = ?,
            expiration_date = ?
            WHERE id = ?;
            """;

    private static final String SQL_DELETE = "DELETE FROM products WHERE id = ?;";

    private final DataSource dataSource;

    public ProductDaoImpl(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void save(final Product product) {
        try(Connection connection = dataSource.getConnection()) {
            save(product, connection);
        } catch(SQLException e) {
            throw new DaoOperationException(String.format("Error saving product: %s", product), e);
        }
    }

    private void save(final Product product, final Connection connection) throws SQLException {
        final PreparedStatement preparedStatement = getInsertStatement(product, connection);
        preparedStatement.executeUpdate();
        final Long generatedId = getGeneratedId(preparedStatement);
        product.setId(generatedId);
    }

    private Long getGeneratedId(final PreparedStatement preparedStatement) throws SQLException {
        final ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
        if(generatedKeys.next()) {
            return generatedKeys.getLong(1);
        } else {
            throw new DaoOperationException("Cannot generate product ID");
        }
    }

    private PreparedStatement getInsertStatement(final Product product, final Connection connection) {
        try {
            final PreparedStatement preparedStatement = connection.prepareStatement(
                    SQL_INSERT,
                    Statement.RETURN_GENERATED_KEYS
            );
            fillPreparedStatementWithProductFields(product, preparedStatement);
            return preparedStatement;
        } catch(SQLException e) {
            throw new DaoOperationException(String.format("Cannot prepare statement for product: %s", product), e);
        }
    }

    private static void fillPreparedStatementWithProductFields(final Product product, final PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1, product.getName());
        preparedStatement.setString(2, product.getProducer());
        preparedStatement.setBigDecimal(3, product.getPrice());
        preparedStatement.setDate(4, Date.valueOf(product.getExpirationDate()));
    }

    @Override
    public List<Product> findAll() {
        try(Connection connection = dataSource.getConnection()) {
            final Statement statement = connection.createStatement();
            final ResultSet resultSet = statement.executeQuery(SQL_SELECT_ALL);
            return getProductList(resultSet);
        } catch(SQLException e) {
            throw new DaoOperationException("Cannot find all products", e);
        }
    }

    private List<Product> getProductList(final ResultSet resultSet) throws SQLException {
        final List<Product> products = new ArrayList<>(resultSet.getFetchSize());
        while(resultSet.next()) {
            products.add(getProduct(resultSet));
        }
        return products;
    }

    private Product getProduct(final ResultSet resultSet) throws SQLException {
        final long id = resultSet.getLong("id");
        final String name = resultSet.getString("name");
        final String producer = resultSet.getString("producer");
        final BigDecimal price = resultSet.getBigDecimal("price");
        final LocalDate expirationDate = resultSet.getDate("expiration_date").toLocalDate();
        final LocalDateTime creationTime = resultSet.getTimestamp("creation_time").toLocalDateTime();
        return new Product(id, name, producer, price, expirationDate, creationTime);
    }

    @Override
    public Product findOne(final Long id) {
        try(Connection connection = dataSource.getConnection()) {
            final PreparedStatement preparedStatement = connection.prepareStatement(SQL_FIND_BY_ID);
            preparedStatement.setLong(1, id);
            final ResultSet resultSet = preparedStatement.executeQuery();
            if(resultSet.next()) {
                return getProduct(resultSet);
            } else {
                throw new DaoOperationException(String.format("Cannot find product with ID=%s", id));
            }
        } catch(SQLException e) {
            throw new DaoOperationException(String.format("Cannot find product with ID=%s", id), e);
        }
    }

    @Override
    public void update(final Product product) {
        if(product.getId() == null) {
            throw new DaoOperationException("Product ID cannot be null");
        }
        try(Connection connection = dataSource.getConnection()) {
            final PreparedStatement preparedStatement = connection.prepareStatement(SQL_UPDATE);
            fillPreparedStatementWithProductFields(product, preparedStatement);
            preparedStatement.setLong(5, product.getId());
            final int rowsAffected = preparedStatement.executeUpdate();
            checkExecuteUpdateResult(rowsAffected, product.getId());
        } catch(SQLException e) {
            throw new DaoOperationException(String.format("Cannot update product: %s", product), e);
        }
    }

    @Override
    public void remove(final Product product) {
        if(product.getId() == null) {
            throw new DaoOperationException("Product ID cannot be null");
        }
        try(Connection connection = dataSource.getConnection()) {
            final PreparedStatement preparedStatement = getDeleteStatement(connection, product);
            final int rowsAffected = preparedStatement.executeUpdate();
            checkExecuteUpdateResult(rowsAffected, product.getId());
        } catch(SQLException e) {
            throw new DaoOperationException(String.format("Cannot delete product with ID=%s", product.getId()), e);
        }
    }

    private PreparedStatement getDeleteStatement(final Connection connection, final Product product) {
        final PreparedStatement preparedStatement;
        try {
            preparedStatement = connection.prepareStatement(SQL_DELETE);
            preparedStatement.setLong(1, product.getId());
        } catch(SQLException e) {
            throw new DaoOperationException(
                    String.format("Cannot prepare statement for product with ID=%s", product.getId()),
                    e
            );
        }
        return preparedStatement;
    }

    private void checkExecuteUpdateResult(final int rowsAffected, final Long productId) {
        if(rowsAffected == 0) {
            throw new DaoOperationException(String.format("Cannot execute operation on product=%s", productId));
        }
    }

}
