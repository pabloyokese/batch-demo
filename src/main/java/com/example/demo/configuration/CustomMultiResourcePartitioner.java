package com.example.demo.configuration;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.core.io.Resource;

import java.util.HashMap;
import java.util.Map;

public class CustomMultiResourcePartitioner implements Partitioner {

    private static final String DEFAULT_KEY_NAME = "fileName";

    private static final String PARTITION_KEY = "partition";

    private Resource[] resources = new Resource[0];

    private String keyName = DEFAULT_KEY_NAME;

    public void setResources(Resource[] resources) {
        this.resources = resources;
    }

    public void setKeyName(String keyName) {
        this.keyName = keyName;
    }

    public Map<String, ExecutionContext> partition(int gridSize) {
        Map<String, ExecutionContext> map = new HashMap<>(gridSize);
        int i = 0, k = 1;
        for (Resource resource : resources) {
            ExecutionContext context = new ExecutionContext();
            context.putString(keyName, resource.getFilename());

            map.put(PARTITION_KEY + i, context);
            i++;
        }
        return map;
    }

}