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
 * - make a setter for field {@link Photo#comments} {@code private}
 * - implement equals() and hashCode() based on identifier field
 * <p>
 * - configure JPA entity
 * - specify table name: "photo"
 * - configure auto generated identifier
 * - configure not nullable and unique column: url
 * <p>
 * - initialize field comments
 * - map relation between Photo and PhotoComment on the child side
 * - implement helper methods {@link Photo#addComment(PhotoComment)} and {@link Photo#removeComment(PhotoComment)}
 * - enable cascade type {@link javax.persistence.CascadeType#ALL} for field {@link Photo#comments}
 * - enable orphan removal
 */
@Getter
@Setter
@EqualsAndHashCode(of = "id")
@NoArgsConstructor
@Entity
@Table(name = "photo")
public class Photo {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    @Column(nullable = false, unique = true)
    private String url;

    private String description;

    @OneToMany(
            mappedBy = "photo",
            cascade = CascadeType.ALL,
            orphanRemoval = true
    )
    private List<PhotoComment> comments = new ArrayList<>();

    private void setComments(final List<PhotoComment> comments) {
        this.comments = comments;
    }

    public void addComment(PhotoComment comment) {
        this.comments.add(comment);
        comment.setPhoto(this);
    }

    public void removeComment(PhotoComment comment) {
        this.comments.remove(comment);
        comment.setPhoto(null);
    }
}
