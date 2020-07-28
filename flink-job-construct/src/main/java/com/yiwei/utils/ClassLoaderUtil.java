package com.yiwei.utils;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by yiwei 2020/7/28
 */
public class ClassLoaderUtil {


    private final static ConcurrentHashMap<String, HotDeployJarClassLoader> LOADER_CACHE = new ConcurrentHashMap<>();


    public void loadJar(String jarName, String jarPath) throws MalformedURLException {
        HotDeployJarClassLoader urlClassLoader = LOADER_CACHE.get(jarName);
        if (urlClassLoader != null) {
            return;
        }
        urlClassLoader = new HotDeployJarClassLoader();
//        String path = "/Users/rongjianmin/work/workspace/flink-sql-utils/target";
        URL jarUrl = new URL("jar:file://" + jarPath + "/" + jarName + "!/");
        urlClassLoader.addURLFile(jarUrl);
        LOADER_CACHE.put(jarName, urlClassLoader);
    }


    public Class loadClass(String jarName, String name) throws ClassNotFoundException {
        HotDeployJarClassLoader urlClassLoader = LOADER_CACHE.get(jarName);
        if (urlClassLoader == null) {
            return null;
        }
        return urlClassLoader.loadClass(name);
    }

    public void unloadJarFile(String jarName, String jarPath) {
        HotDeployJarClassLoader urlClassLoader = LOADER_CACHE.get(jarName);
        if (urlClassLoader == null) {
            return;
        }
//        String path = "/Users/rongjianmin/work/workspace/flink-sql-utils/target";
        String jarStr = "jar:file://" + jarPath + "/" + jarName + "!/";
        urlClassLoader.unloadJarFile(jarStr);
        urlClassLoader = null;
        LOADER_CACHE.remove(jarName);
    }

}
