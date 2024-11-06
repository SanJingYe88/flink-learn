package it.com.entity;

import it.com.util.DateUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@AllArgsConstructor
@NoArgsConstructor
@Data
public class Event {
    private int id;
    private String name;
    private String url;
    private long ts;

    public Event(String name, String url, long ts) {
        this.name = name;
        this.url = url;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "Event(id=" + this.id + ", name=" + this.name + ", url=" + this.url + ", ts=" + DateUtil.formatHHmmss(this.ts) + ")";
    }
}
