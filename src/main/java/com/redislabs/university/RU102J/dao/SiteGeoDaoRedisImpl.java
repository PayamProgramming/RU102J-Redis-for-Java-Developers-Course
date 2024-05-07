package com.redislabs.university.RU102J.dao;

import com.redislabs.university.RU102J.api.Coordinate;
import com.redislabs.university.RU102J.api.GeoQuery;
import com.redislabs.university.RU102J.api.Site;
import redis.clients.jedis.*;

import java.util.*;
import java.util.stream.Collectors;

public class SiteGeoDaoRedisImpl implements SiteGeoDao {
    private JedisPool jedisPool;
    final static private Double capacityThreshold = 0.2;

    public SiteGeoDaoRedisImpl(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    @Override
    public Site findById(long id) {
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, String> fields =
                    jedis.hgetAll(RedisSchema.getSiteHashKey(id));
            if (fields == null || fields.isEmpty()) {
                return null;
            }
            return new Site(fields);
        }
    }

    @Override
    public Set<Site> findAll() {
        try (Jedis jedis = jedisPool.getResource()) {
            Set<String> keys = jedis.zrange(RedisSchema.getSiteGeoKey(), 0, -1);
            Set<Site> sites = new HashSet<>(keys.size());

            // Create a pipeline
            Pipeline pipeline = jedis.pipelined();

            for (String key : keys) {
                // Queue up the hgetAll command in the pipeline
                pipeline.hgetAll(key);
            }

            // Execute the pipeline
            List<Object> responses = pipeline.syncAndReturnAll();

            // Process the responses
            for (Object response : responses) {
                Map<String, String> site = (Map<String, String>) response;
                if (!site.isEmpty()) {
                    sites.add(new Site(site));
                }
            }
            return sites;
        }
    }

    @Override
    public Set<Site> findByGeo(GeoQuery query) {
        if (query.onlyExcessCapacity()) {
            return findSitesByGeoWithCapacity(query);
        } else {
            return findSitesByGeo(query);
        }
    }

    // Challenge #5
    private Set<Site> findSitesByGeoWithCapacity(GeoQuery query) {
        Coordinate coord = query.getCoordinate();
        Double radius = query.getRadius();
        GeoUnit radiusUnit = query.getRadiusUnit();

        try (Jedis jedis = jedisPool.getResource()) {
            List<GeoRadiusResponse> radiusResponses =
                    jedis.georadius(RedisSchema.getSiteGeoKey(), coord.getLng(),
                            coord.getLat(), radius, radiusUnit);

            Set<Site> sites = new HashSet<>();
            for (GeoRadiusResponse response : radiusResponses) {
                String key = response.getMemberByString();
                Map<String, String> siteMap = jedis.hgetAll(key);
                if (siteMap == null || siteMap.isEmpty()) {
                    continue;
                }

                Site site = new Site(siteMap);
                Long siteId = site.getId();
                Long rank = jedis.zrevrank(RedisSchema.getCapacityRankingKey(),
                        String.valueOf(siteId));

                if (rank == null) {
                    continue;
                }

                Double capacity = jedis.zscore(RedisSchema.getCapacityRankingKey(),
                        String.valueOf(siteId));
                if (capacity == null) {
                    continue;
                }

                double excessCapacity = capacity - capacityThreshold;
                if (excessCapacity > 0) {
                    sites.add(site);
                }
            }

            return sites;
        }
    }

    private Set<Site> findSitesByGeo(GeoQuery query) {
        Coordinate coord = query.getCoordinate();
        Double radius = query.getRadius();
        GeoUnit radiusUnit = query.getRadiusUnit();

        try (Jedis jedis = jedisPool.getResource()) {
            List<GeoRadiusResponse> radiusResponses =
                    jedis.georadius(RedisSchema.getSiteGeoKey(), coord.getLng(),
                            coord.getLat(), radius, radiusUnit);

            return radiusResponses.stream()
                    .map(response -> jedis.hgetAll(response.getMemberByString()))
                    .filter(Objects::nonNull)
                    .map(Site::new).collect(Collectors.toSet());
        }
    }

    @Override
    public void insert(Site site) {
         try (Jedis jedis = jedisPool.getResource()) {
             String key = RedisSchema.getSiteHashKey(site.getId());
             jedis.hmset(key, site.toMap());

             if (site.getCoordinate() == null) {
                 throw new IllegalArgumentException("Coordinate required for Geo " +
                         "insert.");
             }
             double longitude = site.getCoordinate().getGeoCoordinate().getLongitude();
             double latitude = site.getCoordinate().getGeoCoordinate().getLatitude();
             jedis.geoadd(RedisSchema.getSiteGeoKey(), longitude, latitude,
                     key);
         }
    }
}
