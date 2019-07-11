**document**

1. reference                参考文档
2. design                      设计文档
3. manage                    管理文档
4. maintenance            运维文档

**source**

1. platform	             大数据平台工程

   - az_dispatch         azkaban调度模块

   - etl_script              etl脚本模块

   - meta_manage     元数据管理模块


# master


# tag
v0.0.1-rc
Release.Candidate

# branch

branch-0.1



# 部署
cd data-etl-sloth/source/platform/ && mvn package

分发源
```sh

data-etl-sloth/source/az_dispatch/target/az_dispatch-1.0-jar-with-dependencies.jar
data-etl-sloth/source/task_monitoring/target/task_monitoring-1.0-jar-with-dependencies.jar
data-etl-sloth/source/meta_manage/target/meta_manage-1.0.jar
data-etl-sloth/source/etl_script

目标文件夹 /home/dispatch/Project/data-etl-sloth/modules


data-etl-sloth/flow
data-etl-sloth/script
data-etl-sloth/deploy.sh

目标文件夹 /home/dispatch/Project/data-etl-sloth/
```

分发目标 至 azkaban 执行节点

```sh

172.17.0.217  218 219
7  8 9
```
