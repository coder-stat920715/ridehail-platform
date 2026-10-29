package com.ridehail.notification.channel;

import com.ridehail.notification.application.Channel;
import com.ridehail.notification.application.NotificationMessage;

/** Strategy interface: one implementation per delivery channel. Swap the simulated ones for FCM/APNs, Twilio, SNS... */
public interface ExternalChannel {
    Channel channel();
    void send(NotificationMessage message);
}
