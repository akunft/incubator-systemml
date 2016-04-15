package org.apache.sysml.runtime.instructions.flink.utils;

public class Paths {

    public static final String BASE_PATH = System.getProperty("user.dir");

    public static final String SCRIPTS = BASE_PATH + "/scripts";

    public static final String CONF = BASE_PATH + "/conf";

    public static String resolveResouce(final String path) {
        return ClassLoader.getSystemClassLoader().getResource(path).getPath();
    }
}
