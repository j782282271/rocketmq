/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.common;

import org.apache.rocketmq.logging.InternalLogger;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Configuration {

    private final InternalLogger log;

    /**
     * 如果本类由broker创建则，本list存储了：
     * brokerConfig, nettyServerConfig, nettyClientConfig, messageStoreConfig的实例引用
     * 当通过控制台或者shell脚本，修改broker配置的时候，会调用本类，遍历此list，将新的属性赋值到各个配置类中，动态改变broke参数
     */
    private List<Object> configObjectList = new ArrayList<Object>(4);
    private String storePath;
    private boolean storePathFromConfig = false;
    private Object storePathObject;
    private Field storePathField;
    private DataVersion dataVersion = new DataVersion();
    private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    /**
     * All properties include configs in object and extend properties.
     * configObjectList这个list中的所有类的所有属性都会被存储到这里，以key value形式
     */
    private Properties allConfigs = new Properties();

    public Configuration(InternalLogger log) {
        this.log = log;
    }

    public Configuration(InternalLogger log, Object... configObjects) {
        this.log = log;
        if (configObjects == null || configObjects.length == 0) {
            return;
        }
        for (Object configObject : configObjects) {
            registerConfig(configObject);
        }
    }

    public Configuration(InternalLogger log, String storePath, Object... configObjects) {
        this(log, configObjects);
        this.storePath = storePath;
    }

    /**
     * register config object
     *
     * @return the current Configuration object
     */
    public Configuration registerConfig(Object configObject) {
        try {
            readWriteLock.writeLock().lockInterruptibly();

            try {
                Properties registerProps = MixAll.object2Properties(configObject);
                merge(registerProps, this.allConfigs);
                configObjectList.add(configObject);
            } finally {
                readWriteLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("registerConfig lock error");
        }
        return this;
    }

    /**
     * 注册配置，当前配置中没有的配置，也会put进来
     */
    public Configuration registerConfig(Properties extProperties) {
        if (extProperties == null) {
            return this;
        }
        try {
            readWriteLock.writeLock().lockInterruptibly();
            try {
                merge(extProperties, this.allConfigs);
            } finally {
                readWriteLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("register lock error. {}" + extProperties);
        }
        return this;
    }

    /**
     * 默认使用this.storePath作为storePath
     * 如果调用了本方法，则会从object的fieldName取出属性值作为storePath
     */
    public void setStorePathFromConfig(Object object, String fieldName) {
        assert object != null;
        try {
            readWriteLock.writeLock().lockInterruptibly();
            try {
                this.storePathFromConfig = true;
                this.storePathObject = object;
                // check
                this.storePathField = object.getClass().getDeclaredField(fieldName);
                assert this.storePathField != null && !Modifier.isStatic(this.storePathField.getModifiers());
                this.storePathField.setAccessible(true);
            } catch (NoSuchFieldException e) {
                throw new RuntimeException(e);
            } finally {
                readWriteLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("setStorePathFromConfig lock error");
        }
    }

    /**
     * 默认使用this.storePath返回
     * 如果从config中取出path标识开启，则从storePathField这个congfig的Field字段取出名为storePathObject的属性值
     */
    private String getStorePath() {
        String realStorePath = null;
        try {
            readWriteLock.readLock().lockInterruptibly();
            try {
                realStorePath = this.storePath;
                if (this.storePathFromConfig) {
                    try {
                        realStorePath = (String) storePathField.get(this.storePathObject);
                    } catch (IllegalAccessException e) {
                        log.error("getStorePath error, ", e);
                    }
                }
            } finally {
                readWriteLock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getStorePath lock error");
        }

        return realStorePath;
    }

    public void setStorePath(final String storePath) {
        this.storePath = storePath;
    }

    /**
     * 更新配置，由控制台或者脚本调用
     * 只更新已存在的配置，不存在的属性火忽略
     * 持久化配置
     */
    public void update(Properties properties) {
        try {
            readWriteLock.writeLock().lockInterruptibly();
            try {
                // the property must be exist when update
                mergeIfExist(properties, this.allConfigs);
                for (Object configObject : configObjectList) {
                    // not allConfigs to update...
                    MixAll.properties2Object(properties, configObject);
                }
                this.dataVersion.nextVersion();
            } finally {
                readWriteLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("update lock error, {}", properties);
            return;
        }

        persist();
    }

    public void persist() {
        try {
            readWriteLock.readLock().lockInterruptibly();
            try {
                String allConfigs = getAllConfigsInternal();
                MixAll.string2File(allConfigs, getStorePath());
            } catch (IOException e) {
                log.error("persist string2File error, ", e);
            } finally {
                readWriteLock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("persist lock error");
        }
    }

    /**
     * 将所有broker属性配置变为String,返回
     */
    public String getAllConfigsFormatString() {
        try {
            readWriteLock.readLock().lockInterruptibly();
            try {
                return getAllConfigsInternal();
            } finally {
                readWriteLock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getAllConfigsFormatString lock error");
        }
        return null;
    }

    public String getDataVersionJson() {
        return this.dataVersion.toJson();
    }

    public Properties getAllConfigs() {
        try {
            readWriteLock.readLock().lockInterruptibly();
            try {
                return this.allConfigs;
            } finally {
                readWriteLock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getAllConfigs lock error");
        }

        return null;
    }

    /**
     * merge所有配置类属性到Properties
     * 将Properties变为String返回
     */
    private String getAllConfigsInternal() {
        StringBuilder stringBuilder = new StringBuilder();
        // reload from config object ?
        for (Object configObject : this.configObjectList) {
            Properties properties = MixAll.object2Properties(configObject);
            if (properties != null) {
                merge(properties, this.allConfigs);
            } else {
                log.warn("getAllConfigsInternal object2Properties is null, {}", configObject.getClass());
            }
        }
        stringBuilder.append(MixAll.properties2String(this.allConfigs));
        return stringBuilder.toString();
    }

    private void merge(Properties from, Properties to) {
        for (Object key : from.keySet()) {
            Object fromObj = from.get(key), toObj = to.get(key);
            if (toObj != null && !toObj.equals(fromObj)) {
                log.info("Replace, key: {}, value: {} -> {}", key, toObj, fromObj);
            }
            to.put(key, fromObj);
        }
    }

    /**
     * 只merge to中含有的属性
     * 如果不相等则覆盖
     */
    private void mergeIfExist(Properties from, Properties to) {
        for (Object key : from.keySet()) {
            if (!to.containsKey(key)) {
                continue;
            }

            Object fromObj = from.get(key), toObj = to.get(key);
            if (toObj != null && !toObj.equals(fromObj)) {
                log.info("Replace, key: {}, value: {} -> {}", key, toObj, fromObj);
            }
            to.put(key, fromObj);
        }
    }

}
