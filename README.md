# cache-helper

在application.yml添加Redis配置信息：

```javascript
redis:
  host: redis.server.com
  port: 6379
  password: ''
  default.db: 0
  maxTotal: 50
  maxActive: 1024
  maxIdle: 10
  minIdle: 2
  maxWait: 5000
  testOnBorrow: false # must be false, this is a bug of jedis
  testOnReturn: false # must be false, this is a bug of jedis
```
