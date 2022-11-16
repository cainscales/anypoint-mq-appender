package com.github.cainscales;


import java.util.Map;

public class AnypointMQMessage {
    private Map<String, String> headers;
    private Map<String, String> properties;
    private String body;

    public AnypointMQMessage(String body, Map<String, String> headers, Map<String, String> properties) {
        this.body = body;
        this.headers = headers;
        this.properties = properties;
    }
}
