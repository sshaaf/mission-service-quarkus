package com.redhat.emergency.response.repository;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import com.redhat.emergency.response.model.Mission;
import io.quarkus.runtime.StartupEvent;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class MissionRepository {

    private static final Logger log = LoggerFactory.getLogger(MissionRepository.class);

    @ConfigProperty(name = "infinispan.cache.name.mission", defaultValue = "mission")
    String cacheName;

    @ConfigProperty(name = "infinispan.cache.create.lazy", defaultValue = "false")
    boolean lazy;

    @Inject
    RemoteCacheManager cacheManager;

    volatile RemoteCache<String, String> missionCache;

    void onStart(@Observes StartupEvent e) {
        // do not initialize the cache at startup when remote cache is not available, e.g. in QuarkusTests
        if (!lazy) {
            log.info("Creating remote cache");
            missionCache = initCache();
        }
    }

    // todo put async, error handling
    public void add(Mission mission) {
        getCache().put(mission.getKey(), mission.toJson());
    }

    private RemoteCache<String, String> getCache() {
        RemoteCache<String, String> cache = missionCache;
        if (cache == null) {
            synchronized(this) {
                if (missionCache == null) {
                    missionCache = cache = initCache();
                }
            }
        }
        return cache;
    }

    private RemoteCache<String, String> initCache() {
        Configuration configuration = Configuration.builder().name("mission").mode("SYNC").owners(2).build();
        return cacheManager.administration().getOrCreateCache(cacheName, configuration);
    }

}
