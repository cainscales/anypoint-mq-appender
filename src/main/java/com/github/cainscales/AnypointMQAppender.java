package com.github.cainscales;

import org.apache.logging.log4j.core.*;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Plugin(
        name = "AnypointMQ",
        category= Core.CATEGORY_NAME,
        elementType = Appender.ELEMENT_TYPE
)
public class AnypointMQAppender extends AbstractAppender {
    private static final int MAX_BATCH_COUNT = 10;
    private static final int MIN_BATCH_COUNT = 1;

    private final String destination;
    private final String clientId;
    private final String clientSecret;
    private final String region;
    private final int batchCount;
    private final int batchInterval;

    private AnypointMQSender sender;
    private Layout<? extends Serializable> layout;

    private AnypointMQAppender(
            String name,
            String destination,
            String clientId,
            String clientSecret,
            String region,
            int batchCount,
            int batchInterval,
            Layout<? extends Serializable> layout,
            Filter filter,
            boolean ignoreExceptions
    ) {
        super(name, filter, layout, ignoreExceptions, Property.EMPTY_ARRAY);
        this.destination = destination;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.region = region;
        this.layout = layout;
        this.batchCount = batchCount;
        this.batchInterval = batchInterval;
        this.sender = new AnypointMQSender(this.clientId,
                this.clientSecret,
                this.region,
                this.destination,
                this.batchCount,
                this.batchInterval,
                LOGGER,
                this.getName());
    }

    @PluginFactory
    public static AnypointMQAppender createAppender(
            @PluginAttribute("name") final String name,
            @PluginAttribute("client_id") final String clientId,
            @PluginAttribute("client_secret") final String clientSecret,
            @PluginElement("Properties") final Property[] properties,
            @PluginAttribute("region") final String region,
            @PluginAttribute(value = "ignoreExceptions", defaultBoolean = true) final String ignoreExceptions,
            @PluginAttribute("destination") final String destination,
            @PluginAttribute("batch_count") final String batchCount,
            @PluginAttribute("batch_interval") final String batchInterval,
            @PluginElement("Layout") Layout<? extends Serializable> layout,
            @PluginElement("Filter") final Filter filter
    ) {
        boolean _b_ignoreExceptions = Boolean.getBoolean(ignoreExceptions);
        boolean _b_error_occurred = false;

        if (name == null) {
            LOGGER.error("No name provided for AnypointMQAppender.");
            _b_error_occurred = true;
        }

        int _batch_count = 0;
        if (batchCount != null) {
            try {
                _batch_count = Integer.parseInt(batchCount);
                if (_batch_count > MAX_BATCH_COUNT) {
                    LOGGER.warn("batch_count for AnypointMQ Appender is larger than max possible.");
                    _batch_count = 10;
                }
                else if (_batch_count < 0) {
                    _batch_count = 1;
                }
            } catch (NumberFormatException e) {
                LOGGER.error("batch_count for AnypointMQAppender is not in a valid number format.");
                _b_error_occurred = true;
            }
        }

        int _batch_interval = 0;
        if (batchInterval != null) {
            try {
                _batch_interval = Integer.parseInt(batchInterval);

                if (_batch_interval < 0) {
                    _batch_interval = 0;
                }
            } catch (NumberFormatException e) {
                LOGGER.error("batch_interval for AnypointMQ is not in a valid number format.");
                _b_error_occurred = true;
            }
        }

        if (clientId == null) {
            LOGGER.error("No client_id provided for AnypointMQAppender.");
            _b_error_occurred = true;
        }

        if (clientSecret == null) {
            LOGGER.error("No client_secret provided for AnypointMQAppender.");
            _b_error_occurred = true;
        }

        if (destination == null) {
            LOGGER.error("No destination provided for AnypointMQAppender.");
            _b_error_occurred = true;
        }

        if (region == null) {
            LOGGER.error("No region provided for AnypointMQAppender.");
            _b_error_occurred = true;
        }

        if (layout == null) {
            layout = PatternLayout.newBuilder()
                    .withPattern("%m")
                    .withCharset(StandardCharsets.UTF_8)
                    .withAlwaysWriteExceptions(true)
                    .withNoConsoleNoAnsi(false)
                    .build();
        }

        if (_b_error_occurred) { return null; }

        return new AnypointMQAppender(
                name, destination,
                clientId, clientSecret,
                region,_batch_count, _batch_interval,
                layout, filter,
                _b_ignoreExceptions
        );

    }

    public void append(LogEvent event) {
        String _event_formatted = layout.toSerializable(event).toString();
        String _message_guid = UUID.randomUUID().toString();

        HashMap<String, String> _message_headers = new HashMap<>();
        _message_headers.put("messageId", _message_guid);

        HashMap<String, String> _message_properties = new HashMap<>();
        _message_properties.put("contentType", "text/plain; charset=UTF-8");

        AnypointMQMessage _anypoint_mq_message = new AnypointMQMessage(_event_formatted, _message_headers, _message_properties);
        this.sender.send(_anypoint_mq_message);
    }

    @Override
    public boolean stop(long timeout, TimeUnit timeUnit) {
        this.sender.stop();
        return super.stop(timeout, timeUnit);
    }

}
