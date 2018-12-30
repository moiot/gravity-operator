Gravity 集群使用 [helm](https://helm.sh/) 来安装和升级

### 环境要求
- Kubernetes 1.11+ with CRD status subroutine

### 安装 Gravity Cluster (TODO 开源后直接放到公共 repo)
```bash
$ cd deploy/k8s/gravity-operator
$ helm install --name gravity-operator ./
```

这个 chart 会使用 helm 在 Kubernetes 集群里面启动 gravity-operator 以及 gravity-admin 管理界面


### 配置

Gravity 集群的配置选项如下

请参考 `deploy/k8s/gravity-operator/values.yaml`

Parameter | Description | Default
--- | --- | ---
`deploymentRules`| array of DeploymentRules which control gravity deployment versions | see [DeploymentRules](#DeploymentRules)  
`operator.image.repository`| image of operator | `docker.mobike.io/database/gravity/operator`
`operator.image.tag`| image tag of operator | `156a28a4`
`admin.image.repository`| image of admin | `docker.mobike.io/database/gravity-admin`
`admin.image.tag`| image tag of admin | `59d44f7c`
`admin.service.nodePort`| node port of admin service | `30066`

### DeploymentRules

DeploymentRule 定义了把集群按照 pipeline 的名字分为多组，每一个组可以使用不同的 Gravity 版本。

系统定义了一个默认的组。

```yaml
  - group: "default"
    pipelines: ["*"]
    image: "docker.mobike.io/database/gravity/gravity:156a28a4"
    command: ["/gravity", "-config=/etc/gravity/config.json"]
```


Parameter | Type | Description
--- | --- | ---
`group`| string | name of the rule
`pipelines`| string array | glob expression of pipeline name to match
`image` | string | image(including tag) to deploy of matched pipeline. will be write to pod template's container image field
`command` | string array| command to run of matched pipeline. will be write to pod template's container command field


## 集群升级

修改 chart 内定义的 values.yaml 文件，并使用 helm 升级集群。