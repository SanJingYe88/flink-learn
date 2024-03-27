package it.com.entity;

import lombok.Data;

@Data
public class User {
    private int id;
    private String name;
    private String country;
    private long ts;
}