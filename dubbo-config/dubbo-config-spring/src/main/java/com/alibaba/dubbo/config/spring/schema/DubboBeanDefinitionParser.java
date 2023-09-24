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
package com.alibaba.dubbo.config.spring.schema;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.extension.ExtensionLoader;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.ReflectUtils;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.config.ArgumentConfig;
import com.alibaba.dubbo.config.ConsumerConfig;
import com.alibaba.dubbo.config.MethodConfig;
import com.alibaba.dubbo.config.ProtocolConfig;
import com.alibaba.dubbo.config.ProviderConfig;
import com.alibaba.dubbo.config.RegistryConfig;
import com.alibaba.dubbo.config.spring.ReferenceBean;
import com.alibaba.dubbo.config.spring.ServiceBean;
import com.alibaba.dubbo.rpc.Protocol;

import org.springframework.beans.PropertyValue;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.config.TypedStringValue;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.support.ManagedMap;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.beans.factory.xml.BeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * AbstractBeanDefinitionParser
 * DubboBean定义解析器
 *
 * @export
 */

public class DubboBeanDefinitionParser implements BeanDefinitionParser {

    private static final Logger logger = LoggerFactory.getLogger(DubboBeanDefinitionParser.class);
    private static final Pattern GROUP_AND_VERION = Pattern.compile("^[\\-.0-9_a-zA-Z]+(\\:[\\-.0-9_a-zA-Z]+)?$");

    //Bean 对象类
    private final Class<?> beanClass;
    // 是否需要 Bean 的 id 属性 。是否需要在Bean对象的编号（id）不存在时，自动生成编号。无需被其他应用引用的配置对象，无需自动生成编号。
    // 例如有<dubbo:reference />
    private final boolean required;

    public DubboBeanDefinitionParser(Class<?> beanClass, boolean required) {
        this.beanClass = beanClass;
        this.required = required;
    }

    /**
     * 解析 XML 元素，生成 BeanDefinition 对象
     *
     * @param element
     * @param parserContext
     * @param beanClass
     * @param required
     * @return
     */
    @SuppressWarnings("unchecked")
    private static BeanDefinition parse(Element element, ParserContext parserContext, Class<?> beanClass, boolean required) {
        // 创建 RootBeanDefinition 对象 设置lazyInit为false
        RootBeanDefinition beanDefinition = new RootBeanDefinition();
        beanDefinition.setBeanClass(beanClass);
        //这里有一个点，就是应用缺省是延迟初始化的，只有引用被注入到其他bean,或者被getBean()获取，才会初始化。如果需要饥饿加载，
        // 也就是说没有人引用也立即生成动态代理，可以配置<dubbo:reference ... init="true" />
        beanDefinition.setLazyInit(false);
        // 开始处理Bean的编号
        // 解析配置对象的id ，若不存在，则进行生成
        String id = element.getAttribute("id");
        if ((id == null || id.length() == 0) && required) {
            // 生成id。 不同的配置对象，会存在不同 ，这里生成的id 规则为 name>特殊规则（处理ProtocolConfig）>interface>class
            String generatedBeanName = element.getAttribute("name");
            if (generatedBeanName == null || generatedBeanName.length() == 0) {
                if (ProtocolConfig.class.equals(beanClass)) {
                    generatedBeanName = "dubbo";
                } else {
                    generatedBeanName = element.getAttribute("interface");
                }
            }
            if (generatedBeanName == null || generatedBeanName.length() == 0) {
                generatedBeanName = beanClass.getName();
            }
            id = generatedBeanName;
            // 若 id 已存在，通过自增序列，解决重复。
            int counter = 2;
            while (parserContext.getRegistry().containsBeanDefinition(id)) {
                id = generatedBeanName + (counter++);
            }
        }
        if (id != null && id.length() > 0) {
            if (parserContext.getRegistry().containsBeanDefinition(id)) {
                throw new IllegalStateException("Duplicate spring bean id " + id);
            }
            // 添加到 Spring 的注册表
            parserContext.getRegistry().registerBeanDefinition(id, beanDefinition);
            // 设置 Bean 的id属性值
            beanDefinition.getPropertyValues().addPropertyValue("id", id);
        }
        // 处理 `<dubbo:protocol` /> 的特殊情况
        if (ProtocolConfig.class.equals(beanClass)) {
            // 例如：<dubbo:service interface="com.alibaba.dubbo.demo.DemoService" protocol="dubbo" ref="demoService"/>
            // 例如：<dubbo:protocol id="dubbo" name="dubbo" port="20880" />
            for (String name : parserContext.getRegistry().getBeanDefinitionNames()) {
                BeanDefinition definition = parserContext.getRegistry().getBeanDefinition(name);
                PropertyValue property = definition.getPropertyValues().getPropertyValue("protocol");
                if (property != null) {
                    Object value = property.getValue();
                    if (value instanceof ProtocolConfig && id.equals(((ProtocolConfig) value).getName())) {
                        definition.getPropertyValues().addPropertyValue("protocol", new RuntimeBeanReference(id));
                    }
                }
            }
            // 开始处理 `<dubbo:service />` 的属性 `class`
        } else if (ServiceBean.class.equals(beanClass)) {
            /**
             * 处理 <dubbo:service class="" /> 场景下的处理，大多数情况下我们不这么使用，
             * 包括官方文档也没提供这种方式的说明。当配置 class 属时，会自动创建 Service Bean 对象，而无需再配置 ref 属性，指向 Service Bean 对象。示例如下：
             * <bean id="demoDAO" class="com.alibaba.dubbo.demo.provider.DemoDAO" />
             * <dubbo:service id="sa" interface="com.alibaba.dubbo.demo.DemoService"  class="com.alibaba.dubbo.demo.provider.DemoServiceImpl">
             *     <property name="demoDAO" ref="demoDAO" />
             * </dubbo:service>
             *   通过这种方式，可以使用 <property /> 标签，设置 Service Bean 的属性。
             */
            // 处理 `class` 属性。例如  <dubbo:service id="sa" interface="com.alibaba.dubbo.demo.DemoService" class="com.alibaba.dubbo.demo.provider.DemoServiceImpl" >
            String className = element.getAttribute("class");
            if (className != null && className.length() > 0) {
                // 创建 Service 的 RootBeanDefinition 对象。相当于内嵌了 <bean class="com.alibaba.dubbo.demo.provider.DemoServiceImpl" />
                RootBeanDefinition classDefinition = new RootBeanDefinition();
                classDefinition.setBeanClass(ReflectUtils.forName(className));
                classDefinition.setLazyInit(false);
                // 解析Service Bean 对象的属性
                parseProperties(element.getChildNodes(), classDefinition);
                // 设置 `<dubbo:service ref="" />` 属性
                beanDefinition.getPropertyValues().addPropertyValue("ref", new BeanDefinitionHolder(classDefinition, id + "Impl"));
            }
        } else if (ProviderConfig.class.equals(beanClass)) {
            // 解析 <dubbo:provider /> 的内嵌子元素 <dubbo:service />
            // 解析内嵌的指向的子 XML 元素
            parseNested(element, parserContext, ServiceBean.class, true, "service", "provider", id, beanDefinition);
        } else if (ConsumerConfig.class.equals(beanClass)) {
            parseNested(element, parserContext, ReferenceBean.class, false, "reference", "consumer", id, beanDefinition);
        }
        Set<String> props = new HashSet<String>();  //已解析的属性集合  在下面的代码中会用到
        // 解析的参数集合
        ManagedMap parameters = null;
        // 获取对应Config的所有方法
        Method[] methods = beanClass.getMethods();
        for (Method setter : methods) {
            String name = setter.getName();
            // setting && public && 唯一参数 过滤方法
            if (name.length() > 3 && name.startsWith("set")
                    && Modifier.isPublic(setter.getModifiers())
                    && setter.getParameterTypes().length == 1) {
                Class<?> type = setter.getParameterTypes()[0];
                // 添加属性到 `props` 中
                String lowerCase = name.substring(3, 4).toLowerCase();
                String substring = name.substring(4);
                String propertyName = lowerCase + substring;
                // 这里再做一次判断，确保得到的方法名是小写的 比如  setName  则 propertyName = "name"
                String property = StringUtils.camelToSplitName(propertyName, "-");
                // 添加属性名到 `props` 中  比如上面两行代码  propertyName = "name"  则 property = "name"
                props.add(property);
                // 判断方法符合 getting && public && 返回类型与 setting 参数类型一致的条件。
                Method getter = null;
                try {
                    getter = beanClass.getMethod("get" + name.substring(3), new Class<?>[0]);
                } catch (NoSuchMethodException e) {
                    try {
                        getter = beanClass.getMethod("is" + name.substring(3), new Class<?>[0]);
                    } catch (NoSuchMethodException e2) {
                        // ignore  如果没有找到对应的getter方法  不抛异常 不做处理
                    }
                }
                // 这里保证了 setter 和 getter 的属性类型一致  不一致则跳过 不做处理
                if (getter == null
                        || !Modifier.isPublic(getter.getModifiers())
                        || !type.equals(getter.getReturnType())) {
                    continue;
                }
                // 以下代码的核心思想  就是 获取到对应Config 的Set方法 然后从xml element中获取到对应的属性值 然后设置到Config对象中

                // 解析 `<dubbo:parameters />` 标签的内容
                /**
                 * 对应的配置类：java.util.Map。同时该标签为 protocol 或 service 或 provider 或 reference 或 consumer
                 * 或 monitor 或 registry 或 metadata-config 或 config-center 的子标签，用于配置自定义参数，该配置项将作为扩展点设置自定义参数使用。
                 */
                // 如果方法名为 `setParameters` ，然后就获取 对应的 <dubbo:xxxx /> 的子标签的内容 ，然后返回parameters
                if ("parameters".equals(property)) {
                    // 解析 `<dubbo:parameter />` 标签的内容
                    parameters = parseParameters(element.getChildNodes(), beanDefinition);
                } else if ("methods".equals(property)) {
                    // 解析 `<dubbo:method />`
                    parseMethods(id, element.getChildNodes(), beanDefinition, parserContext);
                } else if ("arguments".equals(property)) {
                    // 解析 `<dubbo:argument />`
                    parseArguments(id, element.getChildNodes(), beanDefinition, parserContext);
                } else {
                    String value = element.getAttribute(property);
                    if (value != null) {
                        value = value.trim();
                        if (value.length() > 0) {
                            // 不想注册到注册中心的情况，即 `registry=N/A` 。
                            if ("registry".equals(property) && RegistryConfig.NO_AVAILABLE.equalsIgnoreCase(value)) {
                                RegistryConfig registryConfig = new RegistryConfig();
                                registryConfig.setAddress(RegistryConfig.NO_AVAILABLE);
                                beanDefinition.getPropertyValues().addPropertyValue(property, registryConfig);
                            } else if ("registry".equals(property) && value.indexOf(',') != -1) {
                                // 多注册中心的情况
                                parseMultiRef("registries", value, beanDefinition, parserContext);
                            } else if ("provider".equals(property) && value.indexOf(',') != -1) {
                                // 多服务提供者的情况
                                parseMultiRef("providers", value, beanDefinition, parserContext);
                            } else if ("protocol".equals(property) && value.indexOf(',') != -1) {
                                // 多协议的情况
                                parseMultiRef("protocols", value, beanDefinition, parserContext);
                            } else {
                                Object reference;
                                // 处理属性类型为基本属性的情况
                                if (isPrimitive(type)) {
                                    // 兼容性处理
                                    if ("async".equals(property) && "false".equals(value)
                                            || "timeout".equals(property) && "0".equals(value)
                                            || "delay".equals(property) && "0".equals(value)
                                            || "version".equals(property) && "0.0.0".equals(value)
                                            || "stat".equals(property) && "-1".equals(value)
                                            || "reliable".equals(property) && "false".equals(value)) {
                                        // backward compatibility for the default value in old version's xsd
                                        value = null;
                                    }
                                    reference = value;
                                // 处理在 `<dubbo:provider />` 或者 `<dubbo:service />` 上定义了 `protocol` 属性的 兼容性。
                                } else if ("protocol".equals(property)
                                        && ExtensionLoader.getExtensionLoader(Protocol.class).hasExtension(value)
                                        && (!parserContext.getRegistry().containsBeanDefinition(value)
                                        || !ProtocolConfig.class.getName().equals(parserContext.getRegistry().getBeanDefinition(value).getBeanClassName()))) {
                                    if ("dubbo:provider".equals(element.getTagName())) {
                                        logger.warn("Recommended replace <dubbo:provider protocol=\"" + value + "\" ... /> to <dubbo:protocol name=\"" + value + "\" ... />");
                                    }
                                    // backward compatibility
                                    ProtocolConfig protocol = new ProtocolConfig();
                                    protocol.setName(value);
                                    reference = protocol;
                                    // 处理 `onreturn` 属性
                                } else if ("onreturn".equals(property)) {
                                    // 按照 `.` 拆分
                                    int index = value.lastIndexOf(".");
                                    String returnRef = value.substring(0, index);
                                    String returnMethod = value.substring(index + 1);
                                    // 创建 RuntimeBeanReference ，指向回调的对象
                                    reference = new RuntimeBeanReference(returnRef);
                                    beanDefinition.getPropertyValues().addPropertyValue("onreturnMethod", returnMethod);
                                } else if ("onthrow".equals(property)) {
                                    int index = value.lastIndexOf(".");
                                    String throwRef = value.substring(0, index);
                                    String throwMethod = value.substring(index + 1);
                                    reference = new RuntimeBeanReference(throwRef);
                                    // 设置 `onreturnMethod` 到 BeanDefinition 的属性值
                                    beanDefinition.getPropertyValues().addPropertyValue("onthrowMethod", throwMethod);
                                } else if ("oninvoke".equals(property)) {
                                    // 处理 `onthrow` 属性
                                    int index = value.lastIndexOf(".");
                                    String invokeRef = value.substring(0, index);
                                    String invokeRefMethod = value.substring(index + 1);
                                    // 创建 RuntimeBeanReference ，指向回调的对象
                                    reference = new RuntimeBeanReference(invokeRef);
                                    // 设置 `onthrowMethod` 到 BeanDefinition 的属性值
                                    beanDefinition.getPropertyValues().addPropertyValue("oninvokeMethod", invokeRefMethod);
                                    // 通用解析
                                } else {
                                    // 指向的 Service 的 Bean 对象，必须是单例
                                    if ("ref".equals(property) && parserContext.getRegistry().containsBeanDefinition(value)) {
                                        BeanDefinition refBean = parserContext.getRegistry().getBeanDefinition(value);
                                        if (!refBean.isSingleton()) {
                                            throw new IllegalStateException("The exported service ref " + value + " must be singleton! Please set the " + value + " bean scope to singleton, eg: <bean id=\"" + value + "\" scope=\"singleton\" ...>");
                                        }
                                    }
                                    // 创建 RuntimeBeanReference ，指向 Service 的 Bean 对象
                                    reference = new RuntimeBeanReference(value);
                                }
                                // 设置 BeanDefinition 的属性值
                                beanDefinition.getPropertyValues().addPropertyValue(propertyName, reference);
                            }
                        }
                    }
                }
            }
        }
        NamedNodeMap attributes = element.getAttributes();
        int len = attributes.getLength();
        for (int i = 0; i < len; i++) {
            Node node = attributes.item(i);
            String name = node.getLocalName();
            if (!props.contains(name)) {
                if (parameters == null) {
                    parameters = new ManagedMap();
                }
                String value = node.getNodeValue();
                parameters.put(name, new TypedStringValue(value, String.class));
            }
        }
        if (parameters != null) {
            beanDefinition.getPropertyValues().addPropertyValue("parameters", parameters);
        }
        return beanDefinition;
    }

    private static boolean isPrimitive(Class<?> cls) {
        return cls.isPrimitive() || cls == Boolean.class || cls == Byte.class
                || cls == Character.class || cls == Short.class || cls == Integer.class
                || cls == Long.class || cls == Float.class || cls == Double.class
                || cls == String.class || cls == Date.class || cls == Class.class;
    }

    @SuppressWarnings("unchecked")
    private static void parseMultiRef(String property, String value, RootBeanDefinition beanDefinition,
                                      ParserContext parserContext) {
        String[] values = value.split("\\s*[,]+\\s*");
        ManagedList list = null;
        for (int i = 0; i < values.length; i++) {
            String v = values[i];
            if (v != null && v.length() > 0) {
                if (list == null) {
                    list = new ManagedList();
                }
                list.add(new RuntimeBeanReference(v));
            }
        }
        beanDefinition.getPropertyValues().addPropertyValue(property, list);
    }

    /**
     * 解析内嵌的指向的子 XML 元素
     *
     * @param element        父XML元素
     * @param parserContext  Spring解析上下文
     * @param beanClass      内嵌解析子元素的Bean类
     * @param required       是否需要Bean的id属性
     * @param tag            标签
     * @param property       父Bean对象在子元素中的属性名
     * @param ref            指向
     * @param beanDefinition 父Bean 定义对象
     */
    private static void parseNested(Element element, ParserContext parserContext, Class<?> beanClass, boolean required,
                                    String tag, String property, String ref, BeanDefinition beanDefinition) {
        // 获得子元素数组
        NodeList nodeList = element.getChildNodes();
        if (nodeList != null && nodeList.getLength() > 0) {
            boolean first = true;
            for (int i = 0; i < nodeList.getLength(); i++) {
                // 开始遍历子元素
                Node node = nodeList.item(i);
                // 如果是 XML 元素
                if (node instanceof Element) {
                    // 判断是否指定要解析的子元素
                    if (tag.equals(node.getNodeName())
                            || tag.equals(node.getLocalName())) {
                        if (first) {
                            first = false;
                            // todo 这里默认是干嘛的
                            String isDefault = element.getAttribute("default");
                            if (isDefault == null || isDefault.length() == 0) {
                                beanDefinition.getPropertyValues().addPropertyValue("default", "false");
                            }
                        }
                        // 抛开默认那个玩意  其他的子元素开始解析  创建子元素的BeanDefinition对象
                        BeanDefinition subDefinition = parse((Element) node, parserContext, beanClass, required);
                        // 设置子 BeanDefinition ，指向父 BeanDefinition
                        if (subDefinition != null && ref != null && ref.length() > 0) {
                            subDefinition.getPropertyValues().addPropertyValue(property, new RuntimeBeanReference(ref));
                        }
                    }
                }
            }
        }
    }

    /**
     * 解析 <dubbo:service class="" /> 情况下，内涵的 `<property />` 的赋值。
     *
     * @param nodeList       子元素数组
     * @param beanDefinition Bean 定义对象
     */
    private static void parseProperties(NodeList nodeList, RootBeanDefinition beanDefinition) {
        if (nodeList != null && nodeList.getLength() > 0) {
            for (int i = 0; i < nodeList.getLength(); i++) {
                Node node = nodeList.item(i);
                if (node instanceof Element) {
                    if ("property".equals(node.getNodeName())
                            || "property".equals(node.getLocalName())) {
                        String name = ((Element) node).getAttribute("name");
                        if (name != null && name.length() > 0) {
                            String value = ((Element) node).getAttribute("value");
                            String ref = ((Element) node).getAttribute("ref");
                            // value 和 ref 不能同时为空  优先使用 value 其次使用 ref
                            if (value != null && value.length() > 0) {
                                beanDefinition.getPropertyValues().addPropertyValue(name, value);
                            } else if (ref != null && ref.length() > 0) {
                                beanDefinition.getPropertyValues().addPropertyValue(name, new RuntimeBeanReference(ref));
                            } else {
                                throw new UnsupportedOperationException("Unsupported <property name=\"" + name + "\"> sub tag, Only supported <property name=\"" + name + "\" ref=\"...\" /> or <property name=\"" + name + "\" value=\"...\" />");
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * 解析 <dubbo:service class="" /> 情况下，内涵的 `<property />` 的赋值。
     * @param nodeList  子元素数组
     * @param beanDefinition  定义对象
     * @return
     */
    @SuppressWarnings("unchecked")
    private static ManagedMap parseParameters(NodeList nodeList, RootBeanDefinition beanDefinition) {
        if (nodeList != null && nodeList.getLength() > 0) {
            ManagedMap parameters = null;
            for (int i = 0; i < nodeList.getLength(); i++) {
                Node node = nodeList.item(i);
                if (node instanceof Element) {
                    if ("parameter".equals(node.getNodeName())
                            || "parameter".equals(node.getLocalName())) {
                        if (parameters == null) {
                            parameters = new ManagedMap();
                        }
                        String key = ((Element) node).getAttribute("key");
                        String value = ((Element) node).getAttribute("value");
                        boolean hide = "true".equals(((Element) node).getAttribute("hide"));
                        if (hide) {
                            key = Constants.HIDE_KEY_PREFIX + key;
                        }
                        parameters.put(key, new TypedStringValue(value, String.class));
                    }
                }
            }
            return parameters;
        }
        return null;
    }

    /**
     * 解析 `<dubbo:method />`
     * @param id Bean的 `id` 属性
     * @param nodeList  子元素节点数组
     * @param beanDefinition  Bean定义对象
     * @param parserContext 解析上下文
     */
    @SuppressWarnings("unchecked")
    private static void parseMethods(String id, NodeList nodeList, RootBeanDefinition beanDefinition,
                                     ParserContext parserContext) {
        if (nodeList != null && nodeList.getLength() > 0) {
            ManagedList methods = null;  // 解析的方法数组
            for (int i = 0; i < nodeList.getLength(); i++) {
                Node node = nodeList.item(i);
                if (node instanceof Element) {
                    Element element = (Element) node;
                    if ("method".equals(node.getNodeName()) || "method".equals(node.getLocalName())) {
                        String methodName = element.getAttribute("name");
                        if (methodName == null || methodName.length() == 0) {
                            throw new IllegalStateException("<dubbo:method> name attribute == null");
                        }
                        if (methods == null) {
                            methods = new ManagedList();
                        }
                        BeanDefinition methodBeanDefinition = parse(((Element) node),
                                parserContext, MethodConfig.class, false);
                        String name = id + "." + methodName;
                        BeanDefinitionHolder methodBeanDefinitionHolder = new BeanDefinitionHolder(
                                methodBeanDefinition, name);
                        methods.add(methodBeanDefinitionHolder);
                    }
                }
            }
            if (methods != null) {
                beanDefinition.getPropertyValues().addPropertyValue("methods", methods);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void parseArguments(String id, NodeList nodeList, RootBeanDefinition beanDefinition,
                                       ParserContext parserContext) {
        if (nodeList != null && nodeList.getLength() > 0) {
            ManagedList arguments = null;
            for (int i = 0; i < nodeList.getLength(); i++) {
                Node node = nodeList.item(i);
                if (node instanceof Element) {
                    Element element = (Element) node;
                    if ("argument".equals(node.getNodeName()) || "argument".equals(node.getLocalName())) {
                        String argumentIndex = element.getAttribute("index");
                        if (arguments == null) {
                            arguments = new ManagedList();
                        }
                        BeanDefinition argumentBeanDefinition = parse(((Element) node),
                                parserContext, ArgumentConfig.class, false);
                        String name = id + "." + argumentIndex;
                        BeanDefinitionHolder argumentBeanDefinitionHolder = new BeanDefinitionHolder(
                                argumentBeanDefinition, name);
                        arguments.add(argumentBeanDefinitionHolder);
                    }
                }
            }
            if (arguments != null) {
                beanDefinition.getPropertyValues().addPropertyValue("arguments", arguments);
            }
        }
    }

    @Override
    public BeanDefinition parse(Element element, ParserContext parserContext) {
        return parse(element, parserContext, beanClass, required);
    }

}
