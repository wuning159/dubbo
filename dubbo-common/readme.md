dubbo-common 公共逻辑模块：提供工具类和通用模型

什么是通用模型？
com.alibaba.dubbo.common.URL 为例子
所有扩展点参数都包含 URL 参数，URL 作为上下文信息贯穿整个扩展点设计体系。
URL 采用标准格式：protocol://username:password@host:port/path?key=value&key=value 。
