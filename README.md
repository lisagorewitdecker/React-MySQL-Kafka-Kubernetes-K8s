# React-MySQL-Kafka-Kubernetes-K8s-App
Full-stack CRUD application built with React, Node.js/Express, MySQL, Apache Kafka, and Kubernetes.

Before applying the Kubernetes manifests, create the MySQL root-password Secret from a
secure file kept outside the repository:

```sh
kubectl create secret generic mysql-secret --from-file=/secure/path/root-password
```

The MySQL and Kafka PVCs each request 1 GiB from the cluster's default StorageClass.
