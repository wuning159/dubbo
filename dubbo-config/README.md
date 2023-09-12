所有配置最终都将转换为 Dubbo URL 表示，并由服务提供方生成，经注册中心传递给消费方，各属性对应 URL 的参数，参见配置项一览表中的 “对应URL参数” 列。
那么一个 Service 注册到注册中心的格式如下：

dubbo://192.168.3.17:20880/com.alibaba.dubbo.demo.DemoService?anyhost=true&application=demo-provider&default.delay=-1&default.retries=0&default.service.filter=demoFilter&delay=-1&dubbo=2.0.0&generic=false&interface=com.alibaba.dubbo.demo.DemoService&methods=sayHello&pid=19031&side=provider&timestamp=1519651641799
格式为 protocol://username:password@host:port/path?key=value&key=value ，通过 URL#buildString(...) 方法生成。

parameters 属性，参数集合。
从上面的 Service URL 例子我们可以看到，里面的 key=value ，实际上就是 Service 对应的配置项。
该属性，通过 AbstractConfig#appendParameters(parameters, config, prefix) 方法生成。