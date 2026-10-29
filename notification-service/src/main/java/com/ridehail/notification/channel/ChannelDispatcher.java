package com.ridehail.notification.channel;

import com.ridehail.notification.application.Channel;
import com.ridehail.notification.application.NotificationMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * Fans a message out to its external channels. A failing channel is isolated (logged) and never blocks the others or
 * the Kafka consumer; the in-app inbox row is already committed, so the user still sees the notification.
 */
@Component
public class ChannelDispatcher {

    private static final Logger log = LoggerFactory.getLogger(ChannelDispatcher.class);
    private final Map<Channel, ExternalChannel> channels = new EnumMap<>(Channel.class);

    public ChannelDispatcher(List<ExternalChannel> implementations) {
        implementations.forEach(c -> channels.put(c.channel(), c));
    }

    public void deliver(List<NotificationMessage> messages) {
        for (NotificationMessage m : messages) {
            for (Channel ch : m.externalChannels()) {
                try {
                    channels.get(ch).send(m);
                } catch (RuntimeException ex) {
                    log.error("Channel {} failed for user {} ({}): {}", ch, m.userId(), m.type(), ex.toString());
                }
            }
        }
    }
}
