package com.ridehail.notification.channel;

import com.ridehail.notification.application.Channel;
import com.ridehail.notification.application.NotificationMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** SIMULATED push provider (FCM/APNs). Replace the body with a real client; the rest of the service is unchanged. */
@Component
public class PushChannel implements ExternalChannel {
    private static final Logger log = LoggerFactory.getLogger(PushChannel.class);

    @Override public Channel channel() { return Channel.PUSH; }

    @Override
    public void send(NotificationMessage m) {
        log.info("[PUSH ] -> user={} | {} | {}", m.userId(), m.title(), m.body());
    }
}
