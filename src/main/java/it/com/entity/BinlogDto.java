package it.com.entity;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class BinlogDto implements Serializable {
    private String xid;
    private String binlog;
    private String db;
    private String table;
    private String event;
    private List<String> keys;
    private List<BinlogColumnsDto> columns;
    private String time;
    private String canalTime;
}

