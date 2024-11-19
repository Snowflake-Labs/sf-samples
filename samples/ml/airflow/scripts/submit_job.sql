EXECUTE JOB SERVICE
IN COMPUTE POOL ML_DEMO_POOL
FROM SPECIFICATION $$
spec:
  containers:
  - command:
    - python
    - /opt/app/train.py
    image: /snowflake/images/snowflake_images/st_plat/runtime/x86/runtime_image/snowbooks:0.6.0
    name: main
    resources:
      limits:
        cpu: 6000m
        memory: 58Gi
      requests:
        cpu: 6000m
        memory: 58Gi
    volumeMounts:
    - mountPath: /var/log/managedservices/system/mlrs
      name: system-logs
    - mountPath: /var/log/managedservices/user/mlrs
      name: user-logs
    - mountPath: /dev/shm
      name: dshm
    - mountPath: /opt/app
      name: stage-volume
  volumes:
  - name: system-logs
    source: local
  - name: user-logs
    source: local
  - name: dshm
    size: 17Gi
    source: memory
  - name: stage-volume
    source: '@ML_DEMO_STAGE'
$$
NAME = ML_DEMO_JOB;

CALL SYSTEM$GET_SERVICE_LOGS('ML_DEMO_JOB', '0', 'main', 500);