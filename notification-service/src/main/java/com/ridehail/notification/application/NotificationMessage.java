package com.ridehail.notification.application;

import java.util.Set;
import java.util.UUID;

/** A fully rendered notification for ONE recipient. */
public record NotificationMessage(String userId, String type, String title, String body, UUID tripId,
                                  Set<Channel> externalChannels) { }
