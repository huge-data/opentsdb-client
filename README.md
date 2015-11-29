
# OpenTSDB客户端封装

> 基于Java实现。

## 代码说明

`zx.soft.opentsdb.client:` OpenTSDB客户端实现

`zx.soft.opentsdb.metric:` Metric数据格式

`zx.soft.opentsdb.reporter:` Reporter完整实现，使用框架实现
 
`zx.soft.opentsdb.reporter.simple:` Reporter简单实现示例，基于Socket实现

> OpenTsdbReporter使得应用程序可以持续地将Metric数据发送到OpenTSDB服务器上，参考 [2.0 HTTP API](http://opentsdb.net/docs/build/html/api_http/index.html)。

## 使用示例

> [dropwizard](http://dropwizard.io/) 0.7.x应用:

```
    @Override
    public void run(T configuration, Environment environment) throws Exception {
    ...
      OpenTsdbReporter.forRegistry(environment.metrics())
          .prefixedWith("app_name")
          .withTags(ImmutableMap.of("other", "tags"))
          .build(OpenTsdb.forService("http://opentsdb/")
          .create())
          .start(30L, TimeUnit.SECONDS);
```

## 参考

### Metric

> JVM监控数据： (https://github.com/dropwizard/metrics)