# asyncflow-java

## 初始化 mysql

```bash
#
# 初始化 mysql
docker compose up -d
# 检查 db 是否初始化成功
docker exec -it async-flow-db /usr/bin/mysql -uroot -proot@2023 -D asyncflow -e "show tables;"
```

## 启动 flowsvr

## 创建任务

## 启动 worker