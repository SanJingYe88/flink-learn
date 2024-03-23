package it.com.entity;

import lombok.Data;

import java.io.Serializable;

@Data
public class BinlogColumnsDto implements Serializable {
    private String name;
    private String type;
    private String value;
    private Boolean updated;
}
