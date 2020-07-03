package com.redhat.emergency.response.repository;

import javax.enterprise.context.ApplicationScoped;

import io.quarkus.test.Mock;
import org.infinispan.client.hotrod.RemoteCacheManager;

@Mock
@ApplicationScoped
public class MockRemoteCacheManager extends RemoteCacheManager {
}
