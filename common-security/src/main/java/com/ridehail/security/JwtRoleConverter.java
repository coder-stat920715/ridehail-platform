package com.ridehail.security;

import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * IdP-agnostic role mapping: Keycloak (realm_access.roles) and Cognito (cognito:groups).
 * Roles become ROLE_RIDER / ROLE_DRIVER / ROLE_ADMIN. This is the RBAC half of the hybrid model;
 * the ABAC half lives in the services (see TripPermissionEvaluator).
 */
public final class JwtRoleConverter {

    private JwtRoleConverter() { }

    public static JwtAuthenticationConverter create() {
        var converter = new JwtAuthenticationConverter();
        converter.setJwtGrantedAuthoritiesConverter(JwtRoleConverter::extract);
        return converter;
    }

    static Collection<GrantedAuthority> extract(Jwt jwt) {
        Set<String> roles = new LinkedHashSet<>();

        Map<String, Object> realmAccess = jwt.getClaimAsMap("realm_access");
        if (realmAccess != null && realmAccess.get("roles") instanceof Collection<?> c) {
            c.forEach(r -> roles.add(String.valueOf(r)));
        }
        List<String> groups = jwt.getClaimAsStringList("cognito:groups");
        if (groups != null) roles.addAll(groups);

        return roles.stream()
                .map(r -> (GrantedAuthority) new SimpleGrantedAuthority("ROLE_" + r.toUpperCase(Locale.ROOT)))
                .toList();
    }
}
