package com.bobocode.model;

import com.bobocode.util.ExerciseNotCompletedException;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.*;
import java.util.ArrayList;
import java.util.List;

/**
 * todo:
 * - make setter for field {@link Company#products} private
 * - initialize field {@link Company#products} as new {@link ArrayList}
 * - implement a helper {@link Company#addProduct(Product)} that establishes a relation on both sides
 * - implement a helper {@link Company#removeProduct(Product)} that drops a relation on both sides
 * <p>
 * - configure JPA entity
 * - specify table name: "company"
 * - configure auto generated identifier
 * - configure mandatory column "name" for field {@link Company#name}
 * <p>
 * - configure one-to-many relationship as mapped on the child side
 * - override equals() and hashCode() considering entity id
 */
@NoArgsConstructor
@Getter
@Setter
@EqualsAndHashCode(of = "id")
@Entity
@Table(name = "company")
public class Company {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    @Column(nullable = false)
    private String name;

    @OneToMany(mappedBy = "company", fetch = FetchType.LAZY)
    private List<Product> products = new ArrayList<>();

    private void setProducts(final List<Product> products) {
        this.products = products;
    }

    public void addProduct(Product product) {
        this.products.add(product);
        product.setCompany(this);
    }

    public void removeProduct(Product product) {
        product.setCompany(null);
        this.products.remove(product);
    }
}