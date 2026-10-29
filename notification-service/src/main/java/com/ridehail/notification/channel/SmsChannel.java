package com.ridehail.notification.channel;

import com.ridehail.notification.application.Channel;
import com.ridehail.notification.application.NotificationMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** SIMULATED SMS provider (Twilio/SNS). Only used for high-value messages (fare receipt, payment failure). */
@Component
public class SmsChannel implements ExternalChannel {
    private static final Logger log = LoggerFactory.getLogger(SmsChannel.class);

    @Override public Channel channel() { return Channel.SMS; }

    @Override
    public void send(NotificationMessage m) {
        log.info("[SMS  ] -> user={} | {}", m.userId(), m.body());
    }
}
