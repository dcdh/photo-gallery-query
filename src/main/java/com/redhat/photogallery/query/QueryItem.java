package com.redhat.photogallery.query;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity
public class QueryItem extends PanacheEntityBase {

    @Id
    public Long id;
    public String name;
    public String category;
    public int likes;

    @Override
    public String toString() {
        return "QueryItem [id=" + id + ", name=" + name + ", category=" + category + ", likes=" + likes + "]";
    }

}
