En esta rama estuvimos generando una versión para spark 2.1 para intentar solucionar
la problemática de leer de isilon y escribir en local.
En esta versión si se solucionó.


¿Como lo probamos?
1. La imagen que utilizamos es la 10.48.238.129:5000/datiobd/spark-secure-job:1.4.0
2. Esa imagen tenía la 1.6.2 y las pruebas las hacíamos con:
sudo docker run -it -d --entrypoint /bin/bash -e VAULT_PATH=/bbva/es/poc/sas/ingestas -e HADOOP_CONF_URL=http://10.48.238.128:8081/repository/maven-snapshots/com/datio/arq/hdfsconf/R3Dev3/0.1.0-SNAPSHOT/R3Dev3-0.1.0-SNAPSHOT.tar.gz -e SPARK_HISTORY_SERVER_ENABLED=false -e SPARK_PERFORMANCE="--conf spark.cores.max=4 --conf spark.driver.cores=2 --conf spark.executor.memory=1g" -e REALM=ES.DEV3.BBVA.COM -e KADMIN_HOST=ldap.dev3.daas.es.igrupobbva -e VAULT_TOKEN=052b0c3c-36df-0f34-37ac-1b014b6116c9 -e SPARK_JOB_CONF_URL=http://10.48.238.128:8081/repository/maven-snapshots/com/datio/arq/apps/sas/1.0-SNAPSHOT/applicationCsvParquetVsfmapobDev3.conf -e SEC_ENABLED=true -e VAULT_PORT=8200 -e SPARK_DOCKER_IMAGE=10.48.238.129:5000/datiobd/spark-secure-job:1.4.0 -e SPARK_USER=root -e VHOSTS=gosec2.dev3.daas.es.igrupobbva -e SPARK_PRINCIPAL=xspsas1d@ES.DEV3.BBVA.COM -e SPARK_MAIN_CLASS=com.datio.sas.CsvParquet -e KDC_HOST=ldap.dev3.daas.es.igrupobbva -e SPARK_JOB_JAR_URL=http://10.48.238.128:8081/repository/maven-snapshots/com/datio/arq/apps/sas/1.0-SNAPSHOT/sas-1.0-SNAPSHOT-with-dependencies.jar -e KRB5_CONF_URL=http://10.48.238.128:8081/repository/maven-snapshots/com/datio/arq/krb5conf/R3Dev3/0.1.0-SNAPSHOT/R3Dev3-0.1.0-SNAPSHOT.conf -v /opt/mesosphere/lib/:/opt/mesosphere/lib/:ro -v /opt/mesosphere/packages/:/opt/mesosphere/packages/:ro --net host 10.48.238.129:5000/datiobd/spark-secure-job:1.4.0
3. El fichero de configuración que utilizamos fue:
sas {
  master = "local[2]"
  appname = "SAS"

  #CSV Read options
  csv {
    path = "hdfs:///data/sandboxes/sas/out/csv/vsfmapob",
    schema = "vsfmapob"
    options {
      "mode" = "FAILFAST",
      "header" = true
    }
  }
#file:///hola/david/hola hdfs:///data/sandboxes/sas/out/parquet/vsfmapob
  parquet {
    path = "hdfs:///data/sandboxes/sas/out/parquet/vsfmapob"
    savemode = "overwrite"
    options {
    }
  }

}
4. Si escribías a HDFS todo iba bien pero al cambiarlo a file:...fallaba por el error: Exception in thread "main" java.io.IOException: Can't get Master Kerberos principal for use as renewer

5. Nuestra solución fue compilar la versión de spark 2.1 y lo subimos a nexus:
http://10.48.238.128:8081/repository/maven-snapshots/com/datio/arq/spark/1.0-SNAPSHOT/spark-2.1.0-bin-custom-spark-2.1.tgz

6. Dentro de un container de la 1.4.0 la descargamos y hacemos que el enlace simbólico apunte a ella para utilizar esa versión

7. Si ahora ejecutamos la siguiente linea con local, funciona perfectamente
/opt/sds/spark/bin/spark-submit --principal xspsas1d@ES.DEV3.BBVA.COM --keytab /root/sparkJob.keytab --conf spark.executorEnv.HADOOP_CONF_URL=http://10.48.238.128:8081/repository/maven-snapshots/com/datio/arq/hdfsconf/R3Dev3/0.1.0-SNAPSHOT/R3Dev3-0.1.0-SNAPSHOT.tar.gz --conf spark.executorEnv.KRB5_CONF_URL=http://10.48.238.128:8081/repository/maven-snapshots/com/datio/arq/krb5conf/R3Dev3/0.1.0-SNAPSHOT/R3Dev3-0.1.0-SNAPSHOT.conf  --driver-java-options "-Djava.security.krb5.conf=/mnt/mesos/sandbox/krb5.conf" --conf spark.executor.extraJavaOptions="-Djava.security.krb5.conf=/mnt/mesos/sandbox/krb5.conf" --conf spark.cores.max=4 --conf spark.driver.cores=2 --conf spark.executor.memory=1g --master local[4] --files application.conf --conf spark.mesos.executor.docker.image=10.48.238.129:5000/datiobd/sas:1.0.0 --class com.datio.sas.CsvParquet /root/sas-1.0-SNAPSHOT-with-dependencies.jar application.conf

8. Si quisieramos ejecutar en el cluster, deberíamos generar una imagen con la compilación 2.1 de Spark porque sino los executor tendrán una versión distinta.
